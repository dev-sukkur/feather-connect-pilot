"""Alerts (M2 plan 2 O6, design §15.4 and §15.5): the rules M2 has the data for, evaluated by a server loop; an alert
fires once for its subject, is refreshed while the rule still finds it and clears when it doesn't, and its channels
hear of it once, again only when it turns critical. An acknowledged or snoozed alert is quiet.

The rules and their defaults:

| Rule | Fires for | Default |
|---|---|---|
| `run_failed` | a flow whose newest run failed | on in a protected environment |
| `delivery_failed` | a failed delivery older than `after` | `after: 15m` |
| `budget_high` | a budget window at `warning` or `critical` of its units | 0.8 and 0.95 |
| `hold_waiting` | a delivery held for approval longer than `after` | `after: 2h` |
| `schedule_skipped` | a flow whose newest `ticks` scheduled runs were all skipped | `ticks: 3` |
| `dataset_stale` | a dataset whose newest sealed version is older than its `max_lag` | where a dataset has one |

The environment turns a rule on or off, sets its parameters and names its channels (`alert_rule`). The channels are
the project's `settings.alert_channels`: `{name: {kind: webhook | slack | smtp, ...}}`; a rule with no channels of its
own tells every channel.
"""

import json
import logging
import smtplib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.message import EmailMessage
from typing import TYPE_CHECKING, Any

import httpx

from feather_connect.core.strategies import parse_duration
from feather_connect.project import resolve_secret
from feather_connect.storage.port import AlertRow, AlertRuleRow
from feather_server.services.access import require

if TYPE_CHECKING:
    from feather_server.app import AppState
    from feather_server.auth import Actor

log = logging.getLogger(__name__)

RULES: Mapping[str, Mapping[str, Any]] = {
    "run_failed": {},
    "delivery_failed": {"after": "15m"},
    "budget_high": {"warning": 0.8, "critical": 0.95},
    "hold_waiting": {"after": "2h"},
    "schedule_skipped": {"ticks": 3},
    "dataset_stale": {},
}
CHANNEL_KINDS = ("webhook", "slack", "smtp")
LOOKED_AT = 500  # deliveries, runs of a kind, that a pass reads
NOTICE_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class Finding:
    subject: str
    severity: str
    message: str


@dataclass(frozen=True)
class Rule:
    """A rule as it applies here: the default, overridden by what the environment set."""

    name: str
    enabled: bool
    params: Mapping[str, Any]
    channels: tuple[str, ...]
    custom: bool  # the environment set it


def rules(state: "AppState") -> list[Rule]:
    set_here = {r.name: r for r in state.store.alerts.rules()}
    protected = bool(state.project.settings.get("protected"))
    found: list[Rule] = []
    for name, defaults in RULES.items():
        row: AlertRuleRow | None = set_here.get(name)
        if row is None:
            found.append(Rule(name, name != "run_failed" or protected, dict(defaults), (), False))
        else:
            found.append(Rule(name, row.enabled, {**defaults, **row.params}, row.channels, True))
    return found


def check_rule(name: str, params: Mapping[str, Any], channels: Sequence[str], state: "AppState") -> None:
    """Raise `ValueError` for a rule, a parameter or a channel there isn't, or a value of the wrong kind."""
    if name not in RULES:
        raise ValueError(f"{name!r} is not an alert rule: it is one of {', '.join(RULES)}")
    for key, value in params.items():
        if key not in RULES[name]:
            raise ValueError(f"rule {name} has no parameter {key!r}")
        if key == "after":
            parse_duration(str(value))
        elif key in ("warning", "critical") and not (isinstance(value, int | float) and 0 < value <= 1):
            raise ValueError(f"{key} is a share of a window, above 0 and at most 1")
        elif key == "ticks" and not (isinstance(value, int) and value >= 1):
            raise ValueError("ticks is a count of 1 or more")
    known = channels_of(state)
    for channel in channels:
        if channel not in known:
            raise ValueError(f"no alert channel is named {channel!r} in settings.alert_channels")


# The rules.


def _run_failed(state: "AppState", rule: Rule, now: datetime) -> list[Finding]:
    found: list[Finding] = []
    for flow in sorted(_flows(state)):
        runs = [r for r in state.store.runs.list_runs(flow=flow, limit=5) if r.status != "skipped"]
        if runs and runs[0].status == "failed":
            why = f": {runs[0].reason}" if runs[0].reason else ""
            found.append(Finding(f"flow:{flow}", "critical", f"the newest run of {flow} failed{why}"))
    return found


def _since(state: "AppState", route: str, version_id: str, status: str) -> datetime | None:
    moves = [a.at for a in state.store.ledger.attempts(route, version_id) if a.to_status == status]
    return max(moves) if moves else None


def _delivery_failed(state: "AppState", rule: Rule, now: datetime) -> list[Finding]:
    after = parse_duration(str(rule.params["after"]))
    found: list[Finding] = []
    for d in state.store.ledger.list_deliveries(status="failed", limit=LOOKED_AT):
        since = _since(state, d.route, d.version_id, "failed")
        if since is not None and now - since >= after:
            error = f": {d.last_error}" if d.last_error else ""
            found.append(
                Finding(
                    f"route:{d.route}/{d.version_id}",
                    "critical",
                    f"delivery of {d.dataset} {d.partition} to {d.route} failed {_ago(now - since)} ago{error}",
                )
            )
    return found


def _hold_waiting(state: "AppState", rule: Rule, now: datetime) -> list[Finding]:
    after = parse_duration(str(rule.params["after"]))
    found: list[Finding] = []
    for d in state.store.ledger.list_deliveries(status="held", limit=LOOKED_AT):
        since = _since(state, d.route, d.version_id, "held")
        if since is not None and now - since >= after:
            found.append(
                Finding(
                    f"route:{d.route}/{d.version_id}",
                    "warning",
                    f"delivery of {d.dataset} {d.partition} to {d.route} has waited {_ago(now - since)} for a "
                    f"decision: {d.last_error or 'held'}",
                )
            )
    return found


def _budget_high(state: "AppState", rule: Rule, now: datetime) -> list[Finding]:
    warning, critical = float(rule.params["warning"]), float(rule.params["critical"])
    found: list[Finding] = []
    for name in sorted(state.project.budgets):
        try:
            usage = state.store.budgets.usage(name)
        except KeyError:
            continue
        worst = max(
            ((u.used / u.window.units, u) for u in usage if u.window.units), default=None, key=lambda p: p[0]
        )
        if worst is None or worst[0] < warning:
            continue
        share, u = worst
        found.append(
            Finding(
                f"budget:{name}",
                "critical" if share >= critical else "warning",
                f"budget {name} has used {share:.0%} of its {u.window.units} units a {_ago(timedelta(seconds=u.window.seconds))}",
            )
        )
    return found


def _schedule_skipped(state: "AppState", rule: Rule, now: datetime) -> list[Finding]:
    ticks = int(rule.params["ticks"])
    found: list[Finding] = []
    for flow in sorted(_flows(state)):
        runs = [
            r
            for r in state.store.runs.list_runs(flow=flow, limit=LOOKED_AT)
            if r.trigger not in ("manual", "backfill")
        ][:ticks]
        if len(runs) == ticks and all(r.status == "skipped" for r in runs):
            why = runs[0].reason or "skipped"
            found.append(
                Finding(f"flow:{flow}", "warning", f"the last {ticks} ticks of {flow} were skipped: {why}")
            )
    return found


def _dataset_stale(state: "AppState", rule: Rule, now: datetime) -> list[Finding]:
    found: list[Finding] = []
    for name, max_lag in sorted(_lags(state).items()):
        sealed = state.store.versions.last_sealed_at(name)
        lag = parse_duration(max_lag)
        if sealed is not None and now - sealed > lag:
            found.append(
                Finding(
                    f"dataset:{name}",
                    "warning",
                    f"{name} was last read {_ago(now - sealed)} ago, past its max_lag of {max_lag}",
                )
            )
    return found


EVALUATORS: Mapping[str, Callable[["AppState", Rule, datetime], list[Finding]]] = {
    "run_failed": _run_failed,
    "delivery_failed": _delivery_failed,
    "budget_high": _budget_high,
    "hold_waiting": _hold_waiting,
    "schedule_skipped": _schedule_skipped,
    "dataset_stale": _dataset_stale,
}


def _flows(state: "AppState") -> set[str]:
    names = {f.name for f in state.runtime.flows()}
    if state.catalogue is not None:
        names |= {f.name for f in state.catalogue.flows()}
    return names


def _lags(state: "AppState") -> dict[str, str]:
    found: dict[str, str] = {}
    for flow in state.runtime.flows():
        found.update({d.name: d.max_lag for d in flow.datasets if d.max_lag})
    if state.catalogue is not None:
        for flow in state.catalogue.flows():
            try:
                spec = state.store.catalog.flow(flow.name).spec
            except KeyError:
                continue
            found.update(
                {str(d["name"]): str(d["max_lag"]) for d in spec.get("datasets") or [] if d.get("max_lag")}
            )
    return found


def _ago(span: timedelta) -> str:
    seconds = int(span.total_seconds())
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


# The pass.


@dataclass(frozen=True)
class PassReport:
    fired: tuple[AlertRow, ...]
    cleared: tuple[AlertRow, ...]
    notified: tuple[str, ...]  # alert ids whose channels were told
    failed: tuple[str, ...]  # alert ids a channel turned down: retried next pass


def evaluate_once(state: "AppState", send: "Sender | None" = None) -> PassReport:
    """Evaluate every rule that is on, fire and clear its alerts, and tell the channels of the new ones."""
    now = state.clock.now()
    fired: list[AlertRow] = []
    cleared: list[AlertRow] = []
    by_name = {r.name: r for r in rules(state)}
    for rule in by_name.values():
        findings: list[Finding] = []
        if rule.enabled:
            try:
                findings = EVALUATORS[rule.name](state, rule, now)
            except Exception:  # one rule's failure leaves the others' alerts
                log.exception("alert rule %s could not be evaluated", rule.name)
                continue
        for f in findings:
            row, new = state.store.alerts.fire(rule.name, f.subject, f.severity, f.message)
            if new:
                fired.append(row)
        cleared.extend(state.store.alerts.clear(rule.name, [f.subject for f in findings]))
    notified, failed = _notify(state, by_name, now, send or Sender(state))
    return PassReport(tuple(fired), tuple(cleared), tuple(notified), tuple(failed))


def _notify(
    state: "AppState", by_name: Mapping[str, Rule], now: datetime, send: "Sender"
) -> tuple[list[str], list[str]]:
    notified: list[str] = []
    failed: list[str] = []
    channels = channels_of(state)
    for alert in state.store.alerts.unnotified():
        if alert.acked_by is not None or (alert.snoozed_until is not None and alert.snoozed_until > now):
            continue  # quiet: told once it is no longer snoozed, never once acknowledged
        rule = by_name.get(alert.rule)
        names = (rule.channels if rule is not None and rule.channels else tuple(channels)) if channels else ()
        ok = True
        for name in names:
            try:
                send(name, channels[name], alert)
            except Exception as error:  # noqa: BLE001 - a channel's failure is logged and retried
                ok = False
                log.warning("alert %s could not reach channel %s: %s", alert.alert_id, name, error)
        if ok:
            state.store.alerts.mark_notified(alert.alert_id)
            notified.append(alert.alert_id)
        else:
            failed.append(alert.alert_id)
    return notified, failed


# What an operator or a developer does.

SNOOZE_MAX = timedelta(days=7)


class AlertActions:
    """Acknowledge and snooze alerts (`alerts.ack`), and set or reset the environment's rules (`alerts.manage`); each is
    audited."""

    def __init__(self, state: "AppState") -> None:
        self._state = state

    def _audit(self, actor: "Actor", action: str, target: str, request: Mapping[str, Any]) -> None:
        self._state.store.audit.record(actor.kind, actor.name, action, target, dict(request), "ok")

    def ack(self, actor: "Actor", alert_id: str, *, reason: str | None = None) -> AlertRow:
        """Raises `KeyError` for an unknown alert."""
        require(actor, "alerts.ack", self._state.store.audit, f"alert:{alert_id}")
        row = self._state.store.alerts.ack(alert_id, actor.name)
        self._audit(
            actor,
            "alerts.ack",
            f"alert:{alert_id}",
            {"rule": row.rule, "subject": row.subject, "reason": reason},
        )
        return row

    def snooze(self, actor: "Actor", alert_id: str, minutes: int, *, reason: str | None = None) -> AlertRow:
        """Quiet it for `minutes`, or no longer with 0. Raises `KeyError` and `ValueError`."""
        require(actor, "alerts.ack", self._state.store.audit, f"alert:{alert_id}")
        span = timedelta(minutes=minutes)
        if minutes < 0 or span > SNOOZE_MAX:
            raise ValueError(f"a snooze is from 0 minutes to {SNOOZE_MAX.days} days")
        until = self._state.clock.now() + span if minutes else None
        row = self._state.store.alerts.snooze(alert_id, until)
        self._audit(actor, "alerts.snooze", f"alert:{alert_id}", {"minutes": minutes, "reason": reason})
        return row

    def set_rule(
        self,
        actor: "Actor",
        name: str,
        *,
        enabled: bool,
        params: Mapping[str, Any],
        channels: Sequence[str],
        reason: str | None = None,
    ) -> Rule:
        """Raises `ValueError` for a rule, a parameter or a channel there isn't."""
        require(actor, "alerts.manage", self._state.store.audit, f"alert_rule:{name}")
        check_rule(name, params, channels, self._state)
        self._state.store.alerts.set_rule(name, enabled, params, channels)
        self._audit(
            actor,
            "alerts.set_rule",
            f"alert_rule:{name}",
            {"enabled": enabled, "params": dict(params), "channels": list(channels), "reason": reason},
        )
        return next(r for r in rules(self._state) if r.name == name)

    def reset_rule(self, actor: "Actor", name: str, *, reason: str | None = None) -> Rule:
        """Back to the default. Raises `KeyError` for a rule the environment hasn't set."""
        require(actor, "alerts.manage", self._state.store.audit, f"alert_rule:{name}")
        self._state.store.alerts.delete_rule(name)
        self._audit(actor, "alerts.reset_rule", f"alert_rule:{name}", {"reason": reason})
        return next(r for r in rules(self._state) if r.name == name)


# Channels.


def channels_of(state: "AppState") -> dict[str, Mapping[str, Any]]:
    raw = state.project.settings.get("alert_channels") or {}
    return {str(k): v for k, v in raw.items() if isinstance(v, Mapping)} if isinstance(raw, Mapping) else {}


def text_of(alert: AlertRow) -> str:
    return f"[{alert.severity}] {alert.rule}: {alert.message}"


def payload_of(alert: AlertRow) -> dict[str, Any]:
    return {
        "alert_id": alert.alert_id,
        "rule": alert.rule,
        "subject": alert.subject,
        "severity": alert.severity,
        "message": alert.message,
        "fired_at": alert.fired_at.isoformat(),
    }


class Sender:
    """Tells one channel of one alert: a webhook gets the alert as JSON, Slack its text, SMTP an email."""

    def __init__(self, state: "AppState") -> None:
        self._root = state.project.root

    def _secret(self, value: Any) -> str:
        text = str(value)
        return resolve_secret(text, self._root) if text.startswith("${secret:") else text

    def __call__(self, name: str, channel: Mapping[str, Any], alert: AlertRow) -> None:
        kind = channel.get("kind")
        if kind == "webhook":
            answer = httpx.post(
                self._secret(channel["url"]), json=payload_of(alert), timeout=NOTICE_TIMEOUT_S
            )
            answer.raise_for_status()
        elif kind == "slack":
            answer = httpx.post(
                self._secret(channel["url"]), json={"text": text_of(alert)}, timeout=NOTICE_TIMEOUT_S
            )
            answer.raise_for_status()
        elif kind == "smtp":
            message = EmailMessage()
            message["Subject"] = text_of(alert)[:200]
            message["From"] = str(channel["from"])
            to = channel["to"]
            message["To"] = ", ".join(to) if isinstance(to, list) else str(to)
            message.set_content(json.dumps(payload_of(alert), indent=2))
            with smtplib.SMTP(
                str(channel["host"]), int(channel.get("port", 25)), timeout=NOTICE_TIMEOUT_S
            ) as smtp:
                if channel.get("starttls"):
                    smtp.starttls()
                if channel.get("username"):
                    smtp.login(str(channel["username"]), self._secret(channel.get("password", "")))
                smtp.send_message(message)
        else:
            raise ValueError(f"channel {name} is of kind {kind!r}: it is one of {', '.join(CHANNEL_KINDS)}")
