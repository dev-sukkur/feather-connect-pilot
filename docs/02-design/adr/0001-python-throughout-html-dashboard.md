# ADR-0001: Python throughout; server-rendered HTML dashboard

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** OSS Dev1 (sponsor)

## Context

Flows are full code, written by developers and by AI assistants. Integration and ETL work leans on data libraries (Arrow, Polars, DuckDB) and HTTP clients. Both candidate engines, DBOS and Hatchet, have mature Python SDKs. The sponsor asked for an HTML-only front end and a system that is easy to run and maintain.

## Decision

- **Python ≥ 3.12** for the SDK, runtime, connectors, server, CLI and MCP server.
- **The dashboard is server-rendered HTML:**
  - FastAPI with Jinja2 templates;
  - one CSS file;
  - **HTMX**, used only through HTML attributes, for partial refresh.
- There is **no** single-page-app framework, **no** TypeScript and **no** Node toolchain. Charts are server-rendered SVG.

## Consequences

**Benefits:**
- **One language, one toolchain.** The repo uses only Python and `uv`, so contributors and AI assistants need only Python.
- **A strong data ecosystem,** and AI assistants generate Python reliably.
- **The dashboard deploys with the server.** It needs no build, its read views work with JavaScript disabled, and it is easy to secure (CSP, CSRF, auto-escaping).

**Costs:**
- **Raw throughput** is lower than on the JVM or Go. That is acceptable at seconds-level latency and moderate volume (ADR-0005), and the system scales with processes.
- **Rich client-side interactions** (drag-and-drop, complex interactive charts) are harder to build.
- **HTMX is still a small script.** If the sponsor wants literally zero JavaScript, it can be dropped at the cost of full-page reloads.

## Alternatives considered

- **TypeScript everywhere.** Good for single-page apps, but a weaker data ecosystem.
- **Java/Kotlin.** Familiar in enterprises, but heavier to run and slower to iterate on with AI.
- **.NET.** A smaller open-source integration ecosystem.
- **A React/TypeScript dashboard.** Rejected by the sponsor: it adds a second language and a Node build.
- **Python UI frameworks (Streamlit, NiceGUI, Reflex).** Rejected: less control over URLs, accessibility and security, and a websocket-centric design.
