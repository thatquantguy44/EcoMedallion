"""Config-driven alert routing (``config/alerting.yml``).

Same pattern as the ``gold_config/*.py`` loaders: a reviewable YAML file → a
frozen dataclass → validation that fails loudly on a malformed file. Changing
who gets paged is a config edit and a code review, not a deploy.

Two rules this module enforces rather than trusts:

**No secrets in the file.** ``password_env`` / ``client_secret_env`` name an
environment variable; they never hold the value. A config that inlines a
credential is rejected, because this file is checked into git and a rejected
config is a much better outcome than a leaked mailbox password.

**Recipients are validated.** A typo'd address means an alert that silently goes
nowhere, which is worse than no alerting at all — you believe you are covered.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml

DEFAULT_ALERTING_PATH = "config/alerting.yml"

#: Transports understood by :mod:`fred_pipeline.governance.email_transport`.
VALID_TRANSPORTS = ("outlook_smtp", "smtp", "microsoft_graph", "console", "file")

#: When to send. Mirrors the existing ``notify_on`` vocabulary so the email and
#: webhook channels can be reasoned about together.
VALID_POLICIES = ("never", "failure", "always")

# Deliberately permissive: RFC 5322 is not worth implementing here, and the goal
# is catching "joe.bloggs@compnay" style slips and missing @, not certifying
# deliverability.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Keys that must reference an env var rather than carry a value.
_SECRET_HINTS = ("password", "secret", "token", "api_key", "apikey")


class AlertingConfigError(ValueError):
    """Raised when config/alerting.yml is malformed or unsafe."""


def _validate_emails(values: Any, *, where: str) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        raise AlertingConfigError(f"{where} must be a string or a list of strings")
    out = []
    for value in values:
        if not isinstance(value, str) or not _EMAIL_RE.match(value.strip()):
            raise AlertingConfigError(
                f"{where}: {value!r} is not a valid email address. A typo here "
                f"means alerts silently go nowhere."
            )
        out.append(value.strip())
    return tuple(out)


@dataclass(frozen=True)
class TransportConfig:
    """How to send. Credentials are referenced, never contained."""

    kind: str = "console"
    host: str = ""
    port: int = 587
    use_starttls: bool = True
    username: str = ""
    #: Name of the env var holding the password / client secret.
    password_env: str = ""
    tenant_id: str = ""
    client_id: str = ""
    client_secret_env: str = ""
    #: For kind='file' — where to write the rendered message instead of sending.
    output_dir: str = ""
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        if self.kind not in VALID_TRANSPORTS:
            raise AlertingConfigError(
                f"unknown transport kind {self.kind!r}; expected one of "
                f"{list(VALID_TRANSPORTS)}"
            )
        if self.kind in ("outlook_smtp", "smtp"):
            if not self.host:
                raise AlertingConfigError(f"transport {self.kind!r} requires 'host'")
            if not 1 <= int(self.port) <= 65535:
                raise AlertingConfigError(f"invalid port {self.port!r}")
        if self.kind == "microsoft_graph":
            missing = [
                name
                for name, value in (
                    ("tenant_id", self.tenant_id),
                    ("client_id", self.client_id),
                    ("client_secret_env", self.client_secret_env),
                )
                if not value
            ]
            if missing:
                raise AlertingConfigError(
                    f"transport 'microsoft_graph' requires {missing}"
                )
        if self.kind == "file" and not self.output_dir:
            raise AlertingConfigError("transport 'file' requires 'output_dir'")

    def resolve_secret(self) -> str:
        """Read the credential from the environment at send time.

        Returns "" when unset — the transport turns that into a clear error, so
        a missing secret in prod reads as "FRED_SMTP_PASSWORD is not set" rather
        than an SMTP authentication failure.
        """
        env_name = self.client_secret_env or self.password_env
        return os.environ.get(env_name, "") if env_name else ""


@dataclass(frozen=True)
class RouteConfig:
    """Who to tell, for one class of outcome."""

    to: tuple[str, ...] = ()
    cc: tuple[str, ...] = ()
    bcc: tuple[str, ...] = ()

    @property
    def has_recipients(self) -> bool:
        return bool(self.to or self.cc or self.bcc)


@dataclass(frozen=True)
class ECBAssetsAlertConfig:
    """Health thresholds for the dedicated ECB eligible-assets feed.

    Delivery still obeys the parent alerting switch, policy, environment, and
    routes.  This section only decides which ECB conditions are noteworthy.
    """

    enabled: bool = True
    expected_release_time_local: str = "18:15"
    grace_period_minutes: int = 180
    max_change_percent: float = 20.0
    alert_on_gap: bool = True
    alert_on_quarantine: bool = True

    def __post_init__(self) -> None:
        try:
            hour, minute = (
                int(part) for part in self.expected_release_time_local.split(":")
            )
        except (TypeError, ValueError) as exc:
            raise AlertingConfigError(
                "ecb_assets.expected_release_time_local must be HH:MM"
            ) from exc
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise AlertingConfigError(
                "ecb_assets.expected_release_time_local must be HH:MM"
            )
        if self.grace_period_minutes < 0:
            raise AlertingConfigError("ecb_assets.grace_period_minutes must be >= 0")
        if self.max_change_percent < 0:
            raise AlertingConfigError("ecb_assets.max_change_percent must be >= 0")


@dataclass(frozen=True)
class AlertingConfig:
    enabled: bool = False
    policy: str = "failure"
    from_address: str = ""
    from_name: str = "FRED pipeline"
    subject_prefix: str = "[FRED]"
    #: Only alert for these environments; empty = all.
    environments: tuple[str, ...] = ()
    transport: TransportConfig = field(default_factory=TransportConfig)
    routes: dict[str, RouteConfig] = field(default_factory=dict)
    #: Attach the full stage detail as a JSON attachment.
    attach_run_json: bool = False
    #: Cap on failing series listed in the body before truncating.
    max_failures_listed: int = 25
    ecb_assets: ECBAssetsAlertConfig = field(default_factory=ECBAssetsAlertConfig)

    def __post_init__(self) -> None:
        if self.policy not in VALID_POLICIES:
            raise AlertingConfigError(
                f"policy {self.policy!r} must be one of {list(VALID_POLICIES)}"
            )
        if self.enabled and not self.from_address:
            raise AlertingConfigError("alerting is enabled but 'from_address' is empty")

    def route_for(self, verdict: str) -> RouteConfig:
        """Recipients for a verdict, falling back to the 'default' route."""
        return self.routes.get(verdict) or self.routes.get("default") or RouteConfig()

    def should_send(self, verdict: str, environment: str = "") -> bool:
        """Whether an email should go out for this verdict in this environment."""
        if not self.enabled or self.policy == "never":
            return False
        if self.environments and environment and environment not in self.environments:
            return False
        if self.policy == "failure" and verdict == "success":
            return False
        return self.route_for(verdict).has_recipients


def _reject_inline_secrets(raw: dict[str, Any], *, where: str) -> None:
    for key, value in raw.items():
        lowered = key.lower()
        if lowered.endswith("_env"):
            continue
        if any(hint in lowered for hint in _SECRET_HINTS):
            raise AlertingConfigError(
                f"{where}.{key} looks like an inline secret. This file is in "
                f"git — use '{key}_env: NAME_OF_ENV_VAR' instead and set the "
                f"value in the environment."
            )
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            raise AlertingConfigError(
                f"{where}.{key} uses ${{...}} interpolation, which this loader "
                f"does not perform. Use an explicit '*_env' key instead."
            )


def _parse_transport(raw: Any) -> TransportConfig:
    if raw is None:
        return TransportConfig()
    if not isinstance(raw, dict):
        raise AlertingConfigError("'transport' must be a mapping")
    _reject_inline_secrets(raw, where="transport")
    known = {
        "kind",
        "host",
        "port",
        "use_starttls",
        "username",
        "password_env",
        "tenant_id",
        "client_id",
        "client_secret_env",
        "output_dir",
        "timeout_seconds",
    }
    unknown = set(raw) - known
    if unknown:
        raise AlertingConfigError(
            f"transport has unknown field(s): {sorted(unknown)}. "
            f"Allowed: {sorted(known)}"
        )
    return TransportConfig(
        kind=raw.get("kind", "console"),
        host=raw.get("host", ""),
        port=int(raw.get("port", 587)),
        use_starttls=bool(raw.get("use_starttls", True)),
        username=raw.get("username", ""),
        password_env=raw.get("password_env", ""),
        tenant_id=raw.get("tenant_id", ""),
        client_id=raw.get("client_id", ""),
        client_secret_env=raw.get("client_secret_env", ""),
        output_dir=raw.get("output_dir", ""),
        timeout_seconds=int(raw.get("timeout_seconds", 30)),
    )


def _parse_routes(raw: Any) -> dict[str, RouteConfig]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise AlertingConfigError("'routes' must be a mapping of verdict -> recipients")
    valid_keys = {"success", "partial", "failure", "default"}
    unknown = set(raw) - valid_keys
    if unknown:
        raise AlertingConfigError(
            f"routes has unknown key(s): {sorted(unknown)}. "
            f"Allowed: {sorted(valid_keys)}"
        )
    routes: dict[str, RouteConfig] = {}
    for verdict, entry in raw.items():
        if entry is None:
            continue
        if isinstance(entry, (str, list)):
            entry = {"to": entry}
        if not isinstance(entry, dict):
            raise AlertingConfigError(f"routes.{verdict} must be a mapping or a list")
        unknown_fields = set(entry) - {"to", "cc", "bcc"}
        if unknown_fields:
            raise AlertingConfigError(
                f"routes.{verdict} has unknown field(s): {sorted(unknown_fields)}"
            )
        routes[verdict] = RouteConfig(
            to=_validate_emails(entry.get("to"), where=f"routes.{verdict}.to"),
            cc=_validate_emails(entry.get("cc"), where=f"routes.{verdict}.cc"),
            bcc=_validate_emails(entry.get("bcc"), where=f"routes.{verdict}.bcc"),
        )
    return routes


def _parse_ecb_assets(raw: Any) -> ECBAssetsAlertConfig:
    if raw is None:
        return ECBAssetsAlertConfig()
    if not isinstance(raw, dict):
        raise AlertingConfigError("'ecb_assets' must be a mapping")
    known = {
        "enabled",
        "expected_release_time_local",
        "grace_period_minutes",
        "max_change_percent",
        "alert_on_gap",
        "alert_on_quarantine",
    }
    unknown = set(raw) - known
    if unknown:
        raise AlertingConfigError(
            f"ecb_assets has unknown field(s): {sorted(unknown)}. "
            f"Allowed: {sorted(known)}"
        )
    return ECBAssetsAlertConfig(
        enabled=bool(raw.get("enabled", True)),
        expected_release_time_local=str(
            raw.get("expected_release_time_local", "18:15")
        ),
        grace_period_minutes=int(raw.get("grace_period_minutes", 180)),
        max_change_percent=float(raw.get("max_change_percent", 20.0)),
        alert_on_gap=bool(raw.get("alert_on_gap", True)),
        alert_on_quarantine=bool(raw.get("alert_on_quarantine", True)),
    )


def load_alerting_config(path: Optional[str] = None) -> AlertingConfig:
    """Load alert routing from YAML.

    Resolution: explicit ``path``, else ``FRED_ALERTING_FILE``, else
    ``config/alerting.yml``. A missing file returns a disabled config — alerting
    is opt-in, and a pipeline with no alerting file should run normally rather
    than refuse to start. A *malformed* file raises: silently not alerting
    because of a YAML typo is the failure this whole module exists to prevent.
    """
    resolved = path or os.environ.get("FRED_ALERTING_FILE") or DEFAULT_ALERTING_PATH
    if not resolved or not os.path.isfile(resolved):
        return AlertingConfig()
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise AlertingConfigError(f"{resolved} must be a mapping at the top level")

    known = {
        "enabled",
        "policy",
        "from_address",
        "from_name",
        "subject_prefix",
        "environments",
        "transport",
        "routes",
        "attach_run_json",
        "max_failures_listed",
        "ecb_assets",
    }
    unknown = set(data) - known
    if unknown:
        raise AlertingConfigError(
            f"{resolved} has unknown top-level key(s): {sorted(unknown)}. "
            f"Allowed: {sorted(known)}"
        )
    _reject_inline_secrets(
        {k: v for k, v in data.items() if not isinstance(v, (dict, list))},
        where=resolved,
    )

    from_address = data.get("from_address", "")
    if from_address:
        _validate_emails([from_address], where="from_address")

    environments = data.get("environments") or ()
    if isinstance(environments, str):
        environments = [environments]

    return AlertingConfig(
        enabled=bool(data.get("enabled", False)),
        policy=str(data.get("policy", "failure")).lower(),
        from_address=from_address,
        from_name=data.get("from_name", "FRED pipeline"),
        subject_prefix=data.get("subject_prefix", "[FRED]"),
        environments=tuple(str(e) for e in environments),
        transport=_parse_transport(data.get("transport")),
        routes=_parse_routes(data.get("routes")),
        attach_run_json=bool(data.get("attach_run_json", False)),
        max_failures_listed=int(data.get("max_failures_listed", 25)),
        ecb_assets=_parse_ecb_assets(data.get("ecb_assets")),
    )
