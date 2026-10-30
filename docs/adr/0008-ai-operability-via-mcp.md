# ADR-0008: AI operability through MCP, with human-equivalent permissions

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor)

## Context

AI assistants should be able to generate flows, and agents should be able to operate the hub the way a person does: investigate failures, re-run, replay. They must do so safely and accountably.

## Decision

- **AI-friendly SDK and CLI:**
  - `AGENTS.md` conventions;
  - `--json` output on every command;
  - queryable connector descriptions.
- **An MCP server** exposes read, act and authoring tools over the same service layer as the dashboard and API.
  - Act tools are preview-then-confirm.
  - In protected environments, the default agent role can only *request* approval.
- **No LLM calls inside the product** in v1.

## Consequences

**Benefits:**
- Agents can do what operators do, safely and audited.
- No dependency on an LLM provider, and no API keys to manage.

**Costs:** no in-dashboard assistant in v1.

## Alternatives considered

- **An in-dashboard AI assistant.** Deferred to after v1.
- **AI-friendly code only, without MCP.** Agents couldn't operate the hub.
