# Rebuild seed

What a new repository needs to start again from this pilot's specification. The plan and its reasons are in
[the spec extraction and rebuild plan](../docs/08-rebuild/2026-11-11-spec-extraction-and-rebuild-plan.md).

| Here | What it is |
|---|---|
| [`../openspec/specs/`](../openspec/specs) | The specification: 22 capabilities, 240 requirements, 310 scenarios, describing the pilot as it is |
| [`../openspec/traceability.md`](../openspec/traceability.md) | Where each requirement comes from in the pilot, and how sure it is |
| [`stages/`](stages) | The eight build stages. Each file holds a stage's proposal, design and task list |
| [`seed.py`](seed.py) | Turns the specification and the stages into a new repository's `openspec/` folder |

## Seed a new repository

```bash
python rebuild/seed.py ../feather-connect
```

It writes `openspec/config.yaml`, `openspec/traceability.md`, an empty `openspec/specs/`, and eight changes under
`openspec/changes/`, each with the requirements of its capabilities as `## ADDED Requirements`. It refuses a folder
that already has an `openspec/` directory, and it fails if a capability belongs to no stage or to two.

Then, in the new repository:

```bash
openspec validate --all --strict
```

```bash
openspec list
```

Build the changes in order. When a stage's exit criteria are met and CI is green:

```bash
openspec archive 01-core-model
```

That moves the stage's requirements into the new repository's `openspec/specs/`, so `specs/` there always says what
has been built and nothing more.

## Changing the specification before seeding

Edit `openspec/specs/<capability>/spec.md` here, keep its row in `openspec/traceability.md` in step, and check it:

```bash
openspec validate --specs --strict
```

To move a capability to another stage, move its line under "New Capabilities" from one stage file to the other.
