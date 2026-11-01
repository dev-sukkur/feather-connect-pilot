# Feather Connect Open Specification

A language-neutral specification for moving keyed data from sources to many destinations, with one model for batch and events. Anyone can use it to build a compatible runtime, connector or tool, without using the Python code in this repository.

**Status:** 0.1.0-draft. The model may still change before 1.0. **Licence:** MIT.

## What's here

| Path | What it is |
|---|---|
| [feather-connect-spec.md](feather-connect-spec.md) | The specification: concepts, encodings, invariants, state machine and contracts |
| [schemas/](schemas/) | JSON Schemas (draft 2020-12) for flows, connector specs, versions, deliveries, attempts and validation problems |
| [examples/](examples/) | One valid document for each schema |
| [conformance/vectors.json](conformance/vectors.json) | Language-neutral test vectors with exact expected values |

The `spec/` folder stands alone: copy it anywhere to share it.

## How to use it

**To build a compatible implementation:**

1. Implement the encodings in §4 and run them against the vectors. That is the **Encoding** level.
2. Add the delivery state machine, change strategies and budgets for the **Model** level.
3. Keep the invariants through crashes and pass the scenarios in §14 for the **Runtime** level.

**To write a connector:** read §11 and publish a connector spec that fits `schemas/connector-spec.schema.json`.

**To build tools,** such as a catalogue, linter or dashboard: read flows, versions and deliveries through the schemas.

## Reading the vectors

Each top-level key of `vectors.json` is one group of cases.

- `$date`, `$datetime`, `$decimal`, `$bytes` and `$float` tag input values that JSON can't hold.
- 64-bit hashes are decimal strings, so that no reader loses precision. In `content_hash`, turn them back into integers before you sort and encode.
- A `null` expected value means that the input is an error.
- `by_fields` parts given as plain strings are fields keyed by their text.
- Every `budget` case starts with a full budget at a frozen time. `advance_s` moves the clock before the step.
- `retry_delay` uses `rand` to set the jitter: the delay is `W × (0.5 + rand / 2)`, where W is the wait in §9.3.

## How it stays true

`packages/feather-connect/tests/test_open_spec.py` runs the reference implementation against these vectors, schemas and examples in CI. A code change that alters a stored encoding fails there.
