"""`feather server` and `feather worker`: the two long-running processes, and the server's admin commands."""

import logging
import os
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Annotated, NoReturn

import psycopg
import typer
import uvicorn
from sqlalchemy.exc import SQLAlchemyError

from feather_cli.options import DatabaseUrl, Env, Json, ProjectDir
from feather_cli.output import emit, fail
from feather_connect.clock import SystemClock
from feather_connect.core.strategies import parse_duration
from feather_connect.packages.check import protect_server_process
from feather_connect.project import ProjectError
from feather_connect.storage.port import DuplicateUser, Store
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import at_head, migrate
from feather_server.app import create_app
from feather_server.auth import token_digest
from feather_server.services.accounts import CLI, Accounts
from feather_server.settings import Settings, SettingsError

log = logging.getLogger(__name__)

server_app = typer.Typer(
    help="Run the server, or administer it: migrate, create-user, set-role, set-grants, create-token.",
    invoke_without_command=True,
    no_args_is_help=False,
)

_NO_DATABASE_HINT = "set FEATHER_DATABASE_URL to a Postgres URL, or pass --database-url"


def _database(url: str | None, json_mode: bool) -> str:
    if not url:
        fail("FEATHER_DATABASE_URL is not set", hint=_NO_DATABASE_HINT, json_mode=json_mode)
    return url


def _unreachable(error: Exception, json_mode: bool) -> NoReturn:
    reason = str(getattr(error, "orig", error)).strip().splitlines()[0]
    fail(
        f"can't reach the database: {reason}",
        hint="check the URL and that Postgres is running",
        json_mode=json_mode,
    )


def _reachable(url: str, json_mode: bool) -> None:
    """End the command if the database doesn't answer within a few seconds, instead of waiting on a connect."""
    try:
        psycopg.connect(url, connect_timeout=5).close()
    except psycopg.Error as error:
        _unreachable(error, json_mode)


@contextmanager
def _store(url: str | None, json_mode: bool) -> Iterator[Store]:
    """Open the store of a migrated database for a block, or end the command saying what is wrong."""
    url = _database(url, json_mode)
    try:
        current = at_head(url)
    except psycopg.Error as error:
        _unreachable(error, json_mode)
    if not current:
        fail("the database schema is not current", hint="run: feather server migrate", json_mode=json_mode)
    store = connect(url)[0]
    try:
        yield store
    finally:
        store.close()


_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def bind_warnings(host: str, environ: Mapping[str, str]) -> list[str]:
    """What to warn about when the server listens on `host` beyond this machine (Stage 2 M12)."""
    if host in _LOCAL_HOSTS:
        return []
    warned: list[str] = []
    base = environ.get("FEATHER_BASE_URL", "")
    if not base.startswith("https://"):
        warned.append(
            f"listening on {host} with FEATHER_BASE_URL {base or 'unset'}: the session cookie is not Secure. "
            "Set FEATHER_BASE_URL to the https address people use"
        )
    if not environ.get("FEATHER_SECRET_KEY"):
        warned.append(
            "FEATHER_SECRET_KEY is not set: this process signs sessions with a key of its own, so a restart signs "
            "everyone out and replicas do not share sessions"
        )
    return warned


@server_app.callback()
def server(
    ctx: typer.Context,
    host: Annotated[str, typer.Option(help="The address to listen on.")] = "0.0.0.0",
    port: Annotated[int, typer.Option(help="The port to listen on.")] = 8080,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    database_url: DatabaseUrl = None,
    json_mode: Json = False,
) -> None:
    """Run the server: the dashboard, the API and the scheduler. It migrates the database first."""
    if ctx.invoked_subcommand is not None:
        return
    environ = dict(os.environ)
    environ["FEATHER_ENV"] = env
    environ["FEATHER_PROJECT"] = str(project)
    environ.setdefault("FEATHER_BASE_URL", f"http://localhost:{port}")
    if database_url:
        environ["FEATHER_DATABASE_URL"] = database_url
    try:
        settings = Settings.from_env(environ)
    except SettingsError as error:
        fail(str(error), hint=_NO_DATABASE_HINT, json_mode=json_mode)
    _reachable(settings.database_url, json_mode)
    for warning in bind_warnings(host, environ):
        log.warning(warning)
        typer.echo(f"warning: {warning}", err=True)
    try:
        app = create_app(settings)
    except ProjectError as error:
        fail(str(error), hint="run: feather validate", json_mode=json_mode)
    except (psycopg.Error, SQLAlchemyError) as error:
        _unreachable(error, json_mode)
    protect_server_process()  # a load check's child can't read this process's environment (Linux)
    uvicorn.run(app, host=host, port=port, log_config=None)


@server_app.command()
def migrate_database(env: Env = "dev", database_url: DatabaseUrl = None, json_mode: Json = False) -> None:
    """Bring the database schema up to date. Safe to repeat."""
    url = _database(database_url, json_mode)
    _reachable(url, json_mode)
    try:
        migrate(url)
    except (psycopg.Error, SQLAlchemyError) as error:
        _unreachable(error, json_mode)
    emit({"migrated": True}, json_mode=json_mode, human=lambda _: typer.echo("database is up to date"))


server_app.command("migrate")(migrate_database)


@server_app.command("upgrade")
def upgrade_from_project(
    project: ProjectDir = Path("."),
    version: Annotated[
        str | None,
        typer.Option(help="The package version to make it. Default: feather.yaml's version, or 1.0.0."),
    ] = None,
    reason: Annotated[str | None, typer.Option(help="Why, for the audit log.")] = None,
    env: Env = "dev",
    database_url: DatabaseUrl = None,
    json_mode: Json = False,
) -> None:
    """Make an M1 project its environment's first package, with its history: build and install it, rename runs,
    watermarks, deliveries and the ledger to the package's names, and activate its flows. Stop the server and the
    workers first. Safe to repeat: it finishes what a run before left."""
    from feather_cli.upgrade import UpgradeError, upgrade_project
    from feather_connect.packages.manifest import ManifestError
    from feather_server.services.packages import InstallFailed

    url = _database(database_url, json_mode)
    _reachable(url, json_mode)
    try:
        migrate(url)
    except (psycopg.Error, SQLAlchemyError) as error:
        _unreachable(error, json_mode)
    store = connect(url)[0]
    try:
        done = upgrade_project(
            project,
            env=env,
            database_url=url,
            store=store,
            version=version,
            reason=reason or "the upgrade from the project mode",
            note=lambda line: typer.echo(line, err=True),
        )
    except (UpgradeError, ProjectError, ManifestError) as error:
        fail(str(error), json_mode=json_mode)
    except InstallFailed as error:
        fail(f"the {error.check} check failed: {error}", json_mode=json_mode)
    finally:
        store.close()

    def show(found: dict[str, object]) -> None:
        flows = ", ".join(done.activated) or "none new"
        typer.echo(f"{done.package} {done.version} is installed; flows activated: {flows}")
        typer.echo("unset FEATHER_PROJECT and remove the /project mount, then start the server and workers")

    emit(done.as_dict(), json_mode=json_mode, human=show)


def _read_password() -> str:
    if sys.stdin.isatty():
        return typer.prompt("Password", hide_input=True, confirmation_prompt=True)
    return sys.stdin.readline().rstrip("\r\n")


def _accounts(store: Store) -> Accounts:
    return Accounts(store, SystemClock())


@server_app.command()
def create_user(
    username: str,
    role: Annotated[
        str | None,
        typer.Option(
            "--role",
            help="admin or operator. Without it, the first user is an admin and the next ones operators.",
            show_default=False,
        ),
    ] = None,
    env: Env = "dev",
    database_url: DatabaseUrl = None,
    json_mode: Json = False,
) -> None:
    """Add a dashboard user. The password comes from a prompt, or from stdin when it is not a terminal."""
    with _store(database_url, json_mode) as store:
        password = _read_password()
        if not password:
            fail(
                "the password is empty",
                hint="type one at the prompt, or pipe it in on stdin",
                json_mode=json_mode,
            )
        try:
            user = _accounts(store).create_user(CLI, username, password, role)
        except DuplicateUser:
            fail(f"the user {username!r} exists already", json_mode=json_mode)
        except ValueError as error:
            fail(str(error), json_mode=json_mode)
    emit(
        {"user": username, "role": user.role},
        json_mode=json_mode,
        human=lambda _: typer.echo(f"created user {username}, {user.role}"),
    )


@server_app.command()
def set_role(
    username: str,
    role: Annotated[str, typer.Argument(help="admin, developer, operator or viewer.")],
    reason: Annotated[str | None, typer.Option(help="Why, for the audit log.")] = None,
    env: Env = "dev",
    database_url: DatabaseUrl = None,
    json_mode: Json = False,
) -> None:
    """Give a user another role. Their tokens follow it at once: a token never acts above its user's role."""
    with _store(database_url, json_mode) as store:
        try:
            _accounts(store).set_role(CLI, username, role, reason=reason)
        except KeyError:
            fail(f"there is no user {username!r}", json_mode=json_mode)
        except ValueError as error:
            fail(str(error), json_mode=json_mode)
    emit(
        {"user": username, "role": role},
        json_mode=json_mode,
        human=lambda _: typer.echo(f"{username} is now {role}"),
    )


@server_app.command()
def set_grants(
    username: str,
    grants: Annotated[
        list[str] | None,
        typer.Argument(help="approver, pii-reader, both or none: the grants the user has after."),
    ] = None,
    reason: Annotated[str | None, typer.Option(help="Why, for the audit log.")] = None,
    env: Env = "dev",
    database_url: DatabaseUrl = None,
    json_mode: Json = False,
) -> None:
    """Give a user these add-on grants and no others: an approver decides holds, a pii-reader unmasks PII."""
    chosen = list(grants or [])
    with _store(database_url, json_mode) as store:
        try:
            _accounts(store).set_grants(CLI, username, chosen, reason=reason)
        except KeyError:
            fail(f"there is no user {username!r}", json_mode=json_mode)
        except ValueError as error:
            fail(str(error), json_mode=json_mode)
    emit(
        {"user": username, "grants": chosen},
        json_mode=json_mode,
        human=lambda _: typer.echo(f"{username} has {', '.join(chosen) or 'no grants'} now"),
    )


@server_app.command()
def create_token(
    name: str,
    ttl: Annotated[str, typer.Option(help="How long it lasts, such as 90d, 12h or 30m.")] = "90d",
    role: Annotated[
        str | None,
        typer.Option(
            "--role",
            help="admin or operator, no higher than its user's. Without it, its user's role, or operator.",
            show_default=False,
        ),
    ] = None,
    user: Annotated[
        str | None,
        typer.Option(help="The user whose token it is: it never acts above their role. None for automation."),
    ] = None,
    scope: Annotated[
        str,
        typer.Option(help="all, or publish: a token that may only upload packages to the built-in registry."),
    ] = "all",
    env: Env = "dev",
    database_url: DatabaseUrl = None,
    json_mode: Json = False,
) -> None:
    """Create an API token and print it. This is the only time it is shown: the server keeps only its hash."""
    try:
        lifetime: timedelta = parse_duration(ttl)
    except ValueError as error:
        fail(str(error), hint="write the lifetime like 90d, 12h or 30m", json_mode=json_mode)
    with _store(database_url, json_mode) as store:
        try:
            token = _accounts(store).create_token(CLI, name, lifetime, role, user=user, scope=scope)
        except KeyError:
            fail(f"there is no user {user!r}", json_mode=json_mode)
        except ValueError as error:
            fail(str(error), json_mode=json_mode)
        row = store.users.find_token(token_digest(token))
        given = row.role if row is not None else role

    def show(data: dict[str, str]) -> None:
        typer.echo(data["token"])
        typer.echo(f"a {given} token; it is not shown again: store it now", err=True)

    emit(
        {"name": name, "token": token, "ttl": ttl, "role": given, "scope": scope},
        json_mode=json_mode,
        human=show,
    )


def worker(
    env: Env = "dev",
    database_url: DatabaseUrl = None,
    slots: Annotated[int, typer.Option(help="How many tasks each package host runs at once.")] = 8,
    packages: Annotated[
        list[str] | None,
        typer.Option(
            help="The installed packages it runs: a glob of their names, such as 'acme-*'. Repeat for more. "
            "Default: all of them."
        ),
    ] = None,
    project: Annotated[
        Path | None,
        typer.Option(
            "--project",
            envvar="FEATHER_PROJECT",
            hidden=True,
            help="Retired: a worker runs installed packages. Make a project a package with feather server upgrade.",
        ),
    ] = None,
    json_mode: Json = False,
) -> None:
    """Run a worker: a supervisor that runs a package host for each installed package version with work, and
    prepares the versions installed (flow packages §7.1). It imports no flow code itself."""
    from feather_connect.supervisor import run_supervisor

    url = _database(database_url, json_mode)
    if project is not None:
        log.warning(
            "--project and FEATHER_PROJECT are retired: a worker runs the installed packages. Make %s a package with "
            "feather server upgrade --project %s",
            project,
            project,
        )
    _reachable(url, json_mode)
    try:
        code = run_supervisor(env, database_url=url, packages=packages or ["*"], slots=slots)
    except (psycopg.Error, SQLAlchemyError) as error:
        _unreachable(error, json_mode)
    raise typer.Exit(code)
