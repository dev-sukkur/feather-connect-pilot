"""`feather new`: a project, a flow (blank or from a template) or a connector, written whole or not at all.

Everything is written into a temporary folder first and moved into place only once it is complete, so a refusal or a
failure leaves nothing behind. A name that exists is refused; nothing is overwritten.

`--template` copies the template's library function into the flow file as editable code (ruling 12): the project owns
it from then on, and it no longer follows the library's fixes.
"""

import ast
import importlib
import inspect
import keyword
import re
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml

from feather_cli import scaffolds
from feather_cli.options import Json, ProjectDir
from feather_cli.output import emit, fail

new_app = typer.Typer(help="Create a project, a flow or a connector.", no_args_is_help=True)

Name = Annotated[str, typer.Argument(help="Its name: lower case letters, digits, '-' and '_'.")]
Template = Annotated[
    str | None,
    typer.Option("--template", help="A template to copy, such as fan-out-demo.", show_default=False),
]
Target = Annotated[Path, typer.Option("--dir", help="The folder to create the project in.")]

_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
_ENVS = ("dev", "qa", "uat", "prod")
TEMPLATE_PACKAGE = "feather_connect.templates.generic"


def _check_name(name: str, json_mode: bool) -> str:
    """The name as a Python identifier. Refuses one that can't be both a file name and an identifier."""
    ident = name.replace("-", "_")
    if not _NAME.match(name) or keyword.iskeyword(ident) or not ident.isidentifier():
        fail(
            f"{name!r} is not a valid name",
            hint="start with a lower case letter, then use lower case letters, digits, '-' or '_' (at most 63)",
            json_mode=json_mode,
        )
    return ident


def templates_root() -> Path:
    """Where the template projects are: shipped inside this package, or at the top of a checkout."""
    shipped = Path(__file__).resolve().parents[1] / "templates"
    if shipped.is_dir():
        return shipped
    for parent in Path(__file__).resolve().parents:
        if (parent / "templates" / "fan-out-demo" / "feather.yaml").is_file():
            return parent / "templates"
    raise FileNotFoundError("the template projects are not installed with this copy of feather")


def available_templates() -> list[str]:
    root = templates_root()
    return sorted(p.name for p in root.iterdir() if (p / "feather.yaml").is_file())


def _place(staged: Path, final: Path) -> None:
    """Move the staged files into `final`, refusing if any of them is there already. Nothing moves if one is."""
    files = [p for p in staged.rglob("*") if p.is_file()]
    clashes = [
        str((final / p.relative_to(staged)).relative_to(final))
        for p in files
        if (final / p.relative_to(staged)).exists()
    ]
    if clashes:
        raise FileExistsError(", ".join(sorted(clashes)))
    for path in files:
        target = final / path.relative_to(staged)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), target)


def _write(staged: Path, files: Mapping[str, str]) -> None:
    for rel, text in files.items():
        path = staged / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")


# Projects.


@new_app.command("project")
def project(name: Name, directory: Target = Path("."), json_mode: Json = False) -> None:
    """A project with the design's layout: feather.yaml, env/, flows/, connectors/, fixtures/, tests/ and AGENTS.md."""
    ident = _check_name(name, json_mode)
    final = directory / name
    if final.exists():
        fail(f"{final} exists already", hint="choose another name, or another --dir", json_mode=json_mode)
    with tempfile.TemporaryDirectory(dir=directory.resolve() if directory.exists() else None) as tmp:
        staged = Path(tmp) / name
        _write(staged, scaffolds.project_files(name, ident))
        final.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staged), final)
    created = sorted(str(p.relative_to(final)).replace("\\", "/") for p in final.rglob("*") if p.is_file())
    emit(
        {"created": str(final), "files": created},
        json_mode=json_mode,
        human=lambda d: typer.echo(
            f"created {d['created']}; next: cd {name} && feather validate && feather test"
        ),
    )


# Flows.


def _template_source(function: str) -> str:
    """The library module's code that `function` needs: its imports and constants, its helpers and itself."""
    module = importlib.import_module(TEMPLATE_PACKAGE)
    text = inspect.getsource(module)
    tree = ast.parse(text)
    wanted = {function}
    defs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef | ast.ClassDef)}
    queue = [defs[function]]
    while queue:  # the module's own functions that the chosen ones call, and so on
        node = queue.pop()
        for used in ast.walk(node):
            if isinstance(used, ast.Name) and used.id in defs and used.id not in wanted:
                wanted.add(used.id)
                queue.append(defs[used.id])
    lines = text.splitlines()
    kept: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            continue  # the module docstring
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            continue
        if isinstance(node, ast.FunctionDef | ast.ClassDef) and node.name not in wanted:
            continue
        start = min([node.lineno, *(d.lineno for d in getattr(node, "decorator_list", []))])
        end = node.end_lineno or node.lineno
        kept.append("\n".join(lines[start - 1 : end]))
    return "\n\n".join(kept)


def _template_call(template_dir: Path) -> tuple[str, str, str]:
    """From the template project's flow file: its docstring, the library function it calls, and the rest of it."""
    (flow_file,) = [p for p in (template_dir / "flows").glob("*.py") if p.name != "__init__.py"]
    text = flow_file.read_text(encoding="utf-8")
    tree = ast.parse(text)
    function = next(
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == TEMPLATE_PACKAGE
        for alias in node.names
    )
    doc = ast.get_docstring(tree) or ""
    body = [
        line
        for line in text.splitlines()
        if TEMPLATE_PACKAGE not in line and not line.startswith('"""') and line != doc
    ]
    rest = "\n".join(body).strip()
    return doc, function, rest


def _merge_yaml(path: Path, add: Mapping[str, Any], module: str | None) -> str:
    """The YAML file with the connections and budgets of `add` that it lacks, and `module` among its modules.

    What is added is written into the file's text, under its section or as a new section at the end, so the file's
    comments, order and layout stay as they were. Only if that text doesn't read back as the merge (a section written
    in flow style, `connections: {}`, say) is the file written out whole, without its comments."""
    text = path.read_text(encoding="utf-8") if path.is_file() else ""
    found: dict[str, Any] = yaml.safe_load(text) or {} if text.strip() else {}
    want = dict(found)
    out = text
    for section in ("connections", "budgets"):
        have = dict(found.get(section) or {})
        new = {name: s for name, s in (add.get(section) or {}).items() if name not in have}
        if new:
            want[section] = {**have, **new}
            out = _add_to_section(out, section, new)
    if module is not None:
        modules = list(found.get("modules") or [])
        if module not in modules:
            want["modules"] = [*modules, module]
            out = _add_to_section(out, "modules", [module])
    if not text.strip():
        return yaml.safe_dump(want, sort_keys=False)
    try:
        if yaml.safe_load(out) == want:
            return out
    except yaml.YAMLError:
        pass
    return yaml.safe_dump(want, sort_keys=False)


def _add_to_section(text: str, key: str, value: Mapping[str, Any] | list[Any]) -> str:
    """`text` with `value`'s entries added at the end of its top-level block `key:`, indented as that block's entries
    are, or with `key:` and them appended if it has no such block."""
    lines = text.splitlines(keepends=True)
    if isinstance(value, list):  # a list on one line, `modules: [flows.hello]`, takes the items on that line
        inline = re.compile(rf"^({re.escape(key)}:\s*\[)([^\]#]*?)(\s*\].*)$", re.DOTALL)
        for i, line in enumerate(lines):
            m = inline.match(line)
            if m:
                items = [
                    yaml.safe_dump(v, default_flow_style=True).strip().removesuffix("...").strip()
                    for v in value
                ]
                joined = ", ".join([m.group(2).strip(), *items] if m.group(2).strip() else items)
                lines[i] = m.group(1) + joined + m.group(3)
                return "".join(lines)
    head = re.compile(rf"^{re.escape(key)}:\s*(#.*)?$")
    start = next((i for i, line in enumerate(lines) if head.match(line.rstrip("\r\n"))), None)
    if start is None:
        prefix = text if not text or text.endswith("\n") else text + "\n"
        return prefix + yaml.safe_dump({key: value}, sort_keys=False)
    end = len(lines)
    for i in range(start + 1, len(lines)):
        stripped = lines[i].strip()
        # A list's items may start at the key's own column (`- flows.hello`): they are still its block.
        if stripped and not stripped.startswith(("#", "-")) and not lines[i][0].isspace():
            end = i
            break
    content = [i for i in range(start + 1, end) if lines[i].strip() and not lines[i].lstrip().startswith("#")]
    indent = len(lines[content[0]]) - len(lines[content[0]].lstrip()) if content else 2
    after = content[-1] + 1 if content else start + 1
    if after > 0 and not lines[after - 1].endswith("\n"):
        lines[after - 1] += "\n"
    body = yaml.safe_dump(value, sort_keys=False)
    added = "".join(" " * indent + line if line.strip() else line for line in body.splitlines(keepends=True))
    return "".join(lines[:after]) + added + "".join(lines[after:])


def _flow_files(root: Path, name: str, ident: str, template: str | None) -> tuple[dict[str, str], list[str]]:
    """The files a new flow adds or changes, by path, and the ones of them that must not exist yet."""
    module = f"flows.{ident}"
    if template is None:
        files = {
            f"flows/{ident}.py": scaffolds.blank_flow(name, ident),
            f"tests/test_{ident}.py": scaffolds.flow_test(ident, fixtures=False),
            "feather.yaml": _merge_yaml(root / "feather.yaml", scaffolds.BLANK_FLOW_CONNECTIONS, module),
        }
        for env in _ENVS:
            if (root / "env" / f"{env}.yaml").is_file() or env == "dev":
                files[f"env/{env}.yaml"] = _merge_yaml(
                    root / "env" / f"{env}.yaml", scaffolds.BLANK_FLOW_ENV.get(env, {}), None
                )
        return files, [f"flows/{ident}.py", f"tests/test_{ident}.py"]
    source = templates_root() / template
    doc, function, call = _template_call(source)
    flow = f'"""{doc}\n\nCopied from the {template} template by `feather new flow`: this code is the project\'s now."""\n\n'
    flow += _template_source(function) + "\n\n\n" + call + "\n"
    files = {f"flows/{ident}.py": flow, f"tests/test_{ident}.py": scaffolds.flow_test(ident, fixtures=True)}
    template_yaml = yaml.safe_load((source / "feather.yaml").read_text(encoding="utf-8")) or {}
    files["feather.yaml"] = _merge_yaml(root / "feather.yaml", template_yaml, module)
    for env in _ENVS:
        overlay = source / "env" / f"{env}.yaml"
        if overlay.is_file():
            files[f"env/{env}.yaml"] = _merge_yaml(
                root / "env" / f"{env}.yaml", yaml.safe_load(overlay.read_text(encoding="utf-8")) or {}, None
            )
    for fixture in (source / "fixtures").glob("*"):
        if not (root / "fixtures" / fixture.name).exists():
            files[f"fixtures/{fixture.name}"] = fixture.read_text(encoding="utf-8")
    return files, [f"flows/{ident}.py", f"tests/test_{ident}.py"]


@new_app.command("flow")
def flow(
    name: Name, template: Template = None, project: ProjectDir = Path("."), json_mode: Json = False
) -> None:
    """A flow in the project, with its test. `--template` copies a template's code, fixtures and connections."""
    ident = _check_name(name, json_mode)
    if not (project / "feather.yaml").is_file():
        fail(
            f"no feather.yaml in {project.resolve()}",
            hint="run it in a project, or create one with `feather new project <name>`",
            json_mode=json_mode,
        )
    if template is not None and template not in available_templates():
        fail(
            f"there is no template named {template!r}",
            hint=f"the templates are {', '.join(available_templates())}",
            json_mode=json_mode,
        )
    files, fresh = _flow_files(project, name, ident, template)
    taken = [rel for rel in fresh if (project / rel).exists()]
    if taken:
        fail(f"{', '.join(taken)} exists already", hint="choose another name", json_mode=json_mode)
    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp)
        _write(staged, files)
        changed = [rel for rel in files if (project / rel).exists()]
        for rel in changed:  # files that are merged into, rather than added: replaced as a whole, last
            (staged / rel).rename(staged / f"{rel}.merged")
        for path in sorted(p for p in staged.rglob("*") if p.is_file() and not p.name.endswith(".merged")):
            target = project / path.relative_to(staged)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), target)
        for rel in changed:
            shutil.move(str(staged / f"{rel}.merged"), project / rel)
    emit(
        {"flow": name, "template": template, "files": sorted(files)},
        json_mode=json_mode,
        human=lambda d: typer.echo(f"wrote {', '.join(d['files'])}; next: feather validate && feather test"),
    )


# Connectors.


@new_app.command("connector")
def connector(name: Name, project: ProjectDir = Path("."), json_mode: Json = False) -> None:
    """A connector in the project's connectors/ folder: a source and a sink, with conformance tests to fill in."""
    ident = _check_name(name, json_mode)
    if not (project / "feather.yaml").is_file():
        fail(f"no feather.yaml in {project.resolve()}", hint="run it in a project", json_mode=json_mode)
    files = scaffolds.connector_files(name, ident)
    taken = [rel for rel in files if (project / rel).exists() and not rel.endswith("__init__.py")]
    if taken:
        fail(f"{', '.join(taken)} exists already", hint="choose another name", json_mode=json_mode)
    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp)
        _write(staged, {rel: text for rel, text in files.items() if not (project / rel).exists()})
        _place(staged, project)
    emit(
        {"connector": name, "files": sorted(files)},
        json_mode=json_mode,
        human=lambda d: typer.echo(f"wrote {', '.join(d['files'])}; next: feather test"),
    )
