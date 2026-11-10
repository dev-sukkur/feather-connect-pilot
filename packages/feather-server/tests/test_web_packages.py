"""The dashboard's Packages and Registries pages (flow packages §10, task 9): what they show of installed versions, their
checks, environments and hosts, and the actions they take, each asked first, as the roles allow."""

import re
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from feather_connect.storage.port import PackageVersion
from feather_server.app import AppState, create_app
from feather_server.auth import Actor
from feather_server.testing import host_up, prepare, sample_wheel, sign_in

ADMIN = Actor("human", "root", "admin", "root")
NAME, FLOW = "acme-invoices", "acme-invoices/invoices"


@pytest.fixture
def app(state: AppState, tmp_path: Path) -> FastAPI:
    app = create_app(state=state, loops=lambda s: [])
    for version in ("1.0.0", "1.1.0"):
        wheel = sample_wheel(tmp_path / version, version)
        app.state.registries.publish(ADMIN, wheel.name, wheel.read_bytes())
        app.state.packages.install(ADMIN, NAME, version)
        prepare(state, NAME, version)
    return app


def _version(state: AppState, version: str) -> PackageVersion:
    found = state.store.packages.version(NAME, version)
    assert found is not None
    return found


def _csrf(page: str) -> str:
    found = re.search(r'name="csrf_token" value="([^"]+)"', page)
    assert found
    return found.group(1)


def _post(browser: TestClient, page: str, path: str, **form: str) -> httpx.Response:
    return browser.post(path, data={"csrf_token": _csrf(browser.get(page).text), **form})


def test_the_sidebar_has_packages_and_registries(app: FastAPI):
    page = sign_in(app).get("/").text
    assert 'href="/packages"' in page and 'href="/registries"' in page


def test_the_packages_list_shows_versions_active_flows_and_failing_hosts(app: FastAPI, state: AppState):
    state.store.packages.record_host(
        "w1", NAME, "1.1.0", "crash_loop", 5, state.clock.now(), "boom: it fell over"
    )
    page = sign_in(app).get("/packages").text
    assert '<h3>Installed</h3><p class="n">1</p>' in page
    assert '<h3>Failing hosts</h3><p class="n">1</p>' in page and "tile-bad" in page
    assert f'href="/packages/{NAME}"' in page and "1.1.0" in page and "Crash loop" in page
    assert "Install from a registry" not in page  # an operator can't install


def test_an_admin_installs_from_the_page_after_confirming(app: FastAPI, state: AppState, tmp_path: Path):
    wheel = sample_wheel(tmp_path / "2.0.0", "2.0.0")
    app.state.registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    admin = sign_in(app, "boss", "admin")
    assert "Install from a registry" in admin.get("/packages").text
    asked = _post(admin, "/packages", "/packages/install", name=NAME, version="2.0.0", registry="feather")
    assert (
        asked.status_code == 200 and "Install acme-invoices 2.0.0?" in asked.text
    )  # without JavaScript: a 2nd step
    assert state.store.packages.version(NAME, "2.0.0") is None
    done = _post(admin, "/packages", "/packages/install", name=NAME, version="2.0.0", confirm="yes")
    assert done.status_code == 303 and done.headers["location"] == f"/packages/{NAME}?done=installed"
    assert _version(state, "2.0.0").state == "preparing"
    failed = _post(admin, "/packages", "/packages/install", name=NAME, version="9.9.9", confirm="yes")
    assert failed.status_code == 422 and "The fetch check failed" in failed.text
    operator = sign_in(app)
    refused = _post(operator, "/packages", "/packages/install", name=NAME, version="2.0.0", confirm="yes")
    assert refused.status_code == 403


def test_the_package_page_shows_checks_load_check_environment_and_hosts(app: FastAPI, state: AppState):
    now = state.clock.now()
    row = _version(state, "1.0.0")
    state.store.packages.record_env("w1", row.lock_hash, "ready", now)
    state.store.packages.record_host(
        "w1", NAME, "1.0.0", "crash_loop", 4, now, "ImportError: no module named x"
    )
    page = sign_in(app).get(f"/packages/{NAME}").text
    for shown in ("not_installed", "signature", "wheels", "load_check", "webhook_connector", row.sha256):
        assert shown in page
    assert "Crash loop" in page and "ImportError: no module named x" in page and "w1" in page
    assert "Uninstall" not in page  # an operator's view
    assert "Activate<span" in page  # no version in service: an operator may activate one


def test_activate_switch_rollback_and_deactivate_ask_first(app: FastAPI, state: AppState):
    operator = sign_in(app)
    page = f"/packages/{NAME}"
    action = f"/packages/{NAME}/flows/invoices"
    asked = _post(operator, page, f"{action}/activate", version="1.0.0")
    assert asked.status_code == 200 and "Activate acme-invoices/invoices to 1.0.0?" in asked.text
    assert state.store.packages.active() == []
    state.clock.advance(timedelta(minutes=1))  # type: ignore[attr-defined]
    done = _post(operator, page, f"{action}/activate", version="1.0.0", confirm="yes", reason="go live")
    assert done.status_code == 303 and done.headers["location"] == f"{page}?done=activating"
    host_up(state, NAME, "1.0.0")
    state.clock.advance(timedelta(minutes=1))  # type: ignore[attr-defined]
    switch = _post(
        operator, page, f"{action}/activate", version="1.1.0", kind="switch", back="flow", confirm="yes"
    )
    assert switch.status_code == 303 and switch.headers["location"] == f"{action}?done=activating"
    host_up(state, NAME, "1.1.0")
    flow_page = operator.get(action).text
    assert "Roll back to 1.0.0" in flow_page  # 1.0.0 was in service before
    rolled = _post(operator, action, f"{action}/activate", version="1.0.0", kind="rollback", back="flow")
    assert "Roll back acme-invoices/invoices to 1.0.0?" in rolled.text
    _post(
        operator, action, f"{action}/activate", version="1.0.0", kind="rollback", back="flow", confirm="yes"
    )
    host_up(state, NAME, "1.0.0")
    assert [(a.flow, a.version) for a in state.store.packages.active()] == [(FLOW, "1.0.0")]
    off = _post(operator, page, f"{action}/deactivate")
    assert "Deactivate acme-invoices/invoices?" in off.text and state.store.packages.active()
    _post(operator, page, f"{action}/deactivate", confirm="yes")
    assert state.store.packages.active() == []
    actions = [a.action for a in state.store.audit.list_audit()]
    assert actions.count("activate_flow") == 3 and actions.count("deactivate_flow") == 1


def test_an_admin_uninstalls_a_version_not_in_service(app: FastAPI, state: AppState):
    admin = sign_in(app, "boss", "admin")
    page = f"/packages/{NAME}"
    state.store.packages.activate(FLOW, NAME, "1.1.0", "ops", state.clock.now())
    host_up(state, NAME, "1.1.0")
    busy = _post(admin, page, f"{page}/versions/1.1.0/uninstall", confirm="yes")
    assert busy.status_code == 409 and "is active" in busy.text
    asked = _post(admin, page, f"{page}/versions/1.0.0/uninstall")
    assert "Uninstall acme-invoices 1.0.0?" in asked.text
    done = _post(admin, page, f"{page}/versions/1.0.0/uninstall", confirm="yes")
    assert done.status_code == 303
    assert _version(state, "1.0.0").state == "uninstalled"
    assert "Uninstalled" in admin.get(page).text


def test_the_registries_page_lists_adds_tests_and_removes_with_tokens_only_as_set(
    app: FastAPI, state: AppState
):
    operator = sign_in(app)
    shown = operator.get("/registries").text
    assert "feather" in shown and "(built in)" in shown and "Add a registry" not in shown
    admin = sign_in(app, "boss", "admin")
    ref = "${secret:env:PKGS_TOKEN}"
    added = _post(
        admin,
        "/registries",
        "/registries/add",
        name="acme",
        index_url="https://pkgs.example.com/simple/",
        token_ref=ref,
    )
    assert added.status_code == 303 and added.headers["location"] == "/registries?added=acme"  # no resubmit
    shown = admin.get(added.headers["location"]).text
    assert "The registry acme is added." in shown
    assert ref not in shown and ">set<" in shown  # the reference isn't shown, only that there is one
    tested = _post(admin, "/registries", "/registries/feather/test")
    assert tested.status_code == 303
    assert "feather answers" in admin.get(tested.headers["location"]).text
    failed = _post(admin, "/registries", "/registries/acme/test")  # its token's variable isn't set
    assert failed.status_code == 303
    shown = admin.get(failed.headers["location"]).text
    assert "acme: " in shown and "notice-bad" in shown and "acme answers" not in shown
    asked = _post(admin, "/registries", "/registries/acme/remove")
    assert "Remove the registry acme?" in asked.text and state.store.registry.get_registry("acme") is not None
    removed = _post(admin, "/registries", "/registries/acme/remove", confirm="yes")
    assert removed.status_code == 303 and state.store.registry.get_registry("acme") is None
    assert "The registry acme is removed." in admin.get(removed.headers["location"]).text
    bad = _post(
        admin,
        "/registries",
        "/registries/add",
        name="x",
        index_url="https://x.example/simple/",
        token_ref="tok",
    )
    assert bad.status_code == 422 and 'value="tok"' not in bad.text  # a token typed in is never shown back
    assert _post(operator, "/registries", "/registries/feather/test").status_code == 403


def test_a_package_or_flow_that_isnt_installed_is_not_found(app: FastAPI):
    browser = sign_in(app)
    assert browser.get("/packages/nothing").status_code == 404
    assert browser.get(f"/packages/{NAME}/flows/nothing").status_code == 404


# The P5 review.


def test_an_operator_may_not_uninstall_from_the_page(app: FastAPI, state: AppState):
    operator = sign_in(app)
    page = f"/packages/{NAME}"
    refused = _post(operator, page, f"{page}/versions/1.0.0/uninstall", confirm="yes")
    assert refused.status_code == 403 and _version(state, "1.0.0").state == "ready"


def test_a_hostile_load_check_or_host_error_is_shown_escaped(app: FastAPI, state: AppState, tmp_path: Path):
    from feather_connect.packages.check import LoadCheck

    hostile = "<script>alert(1)</script>"
    wheel = sample_wheel(tmp_path / "2.0.0", "2.0.0")
    app.state.registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    app.state.packages.install(ADMIN, NAME, "2.0.0")

    def failing(*_: object, **__: object) -> LoadCheck:
        return LoadCheck(False, [f'{NAME}/<img src=x onerror="alert(2)">'], error=hostile)

    prepare(state, NAME, "2.0.0", checker=failing)
    state.store.packages.record_host("w1", NAME, "2.0.0", "crash_loop", 1, state.clock.now(), hostile)
    page = sign_in(app).get(f"/packages/{NAME}").text
    assert hostile not in page and "&lt;script&gt;alert(1)&lt;/script&gt;" in page
    assert "<img src=x" not in page


def test_the_version_card_shows_where_it_was_promoted_from_and_the_checks_it_passed(
    app: FastAPI, state: AppState, tmp_path: Path
):
    import hashlib

    content = sample_wheel(tmp_path / "3.0.0", "3.0.0")
    data = content.read_bytes()
    app.state.packages.promote(
        ADMIN, content.name, data, sha256=hashlib.sha256(data).hexdigest(), source="qa"
    )
    page = sign_in(app).get(f"/packages/{NAME}").text
    assert "promoted from <b>qa</b> by root" in page
    assert _version(state, "3.0.0").checks and "not_installed" in page


def test_a_rollback_goes_only_to_a_version_that_served(app: FastAPI, state: AppState):
    operator = sign_in(app)
    action = f"/packages/{NAME}/flows/invoices"
    _post(operator, action, f"{action}/activate", version="1.0.0", confirm="yes")  # its host never comes up
    state.clock.advance(timedelta(minutes=1))  # type: ignore[attr-defined]
    _post(operator, action, f"{action}/activate", version="1.1.0", kind="switch", back="flow", confirm="yes")
    host_up(state, NAME, "1.1.0")
    assert "Roll back to" not in operator.get(action).text  # 1.0.0 never served


def test_activating_an_unsigned_version_where_signing_is_now_required_says_so(app: FastAPI, state: AppState):
    from feather_connect.packages.signing import Signer, public_key_text
    from feather_connect.storage.port import EnvEntry

    now = state.clock.now()
    key = public_key_text(Signer.generate().public)
    state.store.env_config.put(EnvEntry("dev", "setting", "trusted_package_keys", [key], "root", now))
    state.store.env_config.put(EnvEntry("dev", "setting", "require_signed_packages", True, "root", now))
    page = f"/packages/{NAME}"
    refused = _post(sign_in(app), page, f"{page}/flows/invoices/activate", version="1.0.0", confirm="yes")
    assert refused.status_code == 422 and "The signature check failed" in refused.text
    assert state.store.packages.active() == []
