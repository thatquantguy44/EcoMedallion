"""Email transports for run alerts — Outlook/Microsoft 365, generic SMTP, and
two non-sending sinks for development.

Every transport implements one method, :meth:`EmailTransport.send`, so the
alerting layer never knows which one it has. That is what makes ``console``
usable as the default: a pipeline with no mail configuration still produces the
summary, in the log, instead of failing or silently doing nothing.

Third-party imports (``requests`` for Graph) happen inside methods, and
``smtplib`` is stdlib, so importing this module costs nothing.

**Credentials are never arguments here.** A transport reads its secret from the
environment variable named in ``config/alerting.yml`` at send time, so a
credential cannot end up in a config file, a log line, or a stack trace.
"""

from __future__ import annotations

import base64
import logging
import os
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formataddr, make_msgid
from pathlib import Path
from typing import Any, Optional, Protocol, Sequence

from fred_pipeline.governance.alerting_config import AlertingConfig, TransportConfig

log = logging.getLogger("fred_pipeline.alerting")


class EmailSendError(RuntimeError):
    """Raised when a transport cannot deliver. Callers decide whether that is
    fatal; the pipeline treats it as non-fatal by design (an alerting failure
    must never fail a run that otherwise succeeded)."""


@dataclass(frozen=True)
class EmailMessageSpec:
    """A rendered message, independent of how it will be sent."""

    subject: str
    text_body: str
    html_body: str
    to: Sequence[str]
    cc: Sequence[str] = ()
    bcc: Sequence[str] = ()
    from_address: str = ""
    from_name: str = ""
    attachments: Sequence[tuple[str, bytes, str]] = ()  # (filename, data, mime)

    @property
    def all_recipients(self) -> list[str]:
        return [*self.to, *self.cc, *self.bcc]


class EmailTransport(Protocol):
    def send(self, message: EmailMessageSpec) -> None: ...


def _build_mime(message: EmailMessageSpec) -> EmailMessage:
    """Multipart/alternative with both parts. Outlook prefers HTML; anything
    text-only still gets a readable summary."""
    mime = EmailMessage()
    mime["Subject"] = message.subject
    mime["From"] = (
        formataddr((message.from_name, message.from_address))
        if message.from_name
        else message.from_address
    )
    mime["To"] = ", ".join(message.to)
    if message.cc:
        mime["Cc"] = ", ".join(message.cc)
    mime["Message-ID"] = make_msgid(domain="fred-pipeline.local")
    # Bcc is deliberately NOT set as a header -- it is passed at the envelope
    # level by the SMTP transport, otherwise every recipient sees the bcc list.
    mime.set_content(message.text_body)
    mime.add_alternative(message.html_body, subtype="html")
    for filename, data, mime_type in message.attachments:
        maintype, _, subtype = mime_type.partition("/")
        mime.add_attachment(
            data,
            maintype=maintype or "application",
            subtype=subtype or "octet-stream",
            filename=filename,
        )
    return mime


class SmtpEmailTransport:
    """SMTP with STARTTLS — the path for Outlook / Microsoft 365 and for any
    other SMTP relay.

    For Microsoft 365 the host is ``smtp.office365.com`` on port 587. Note that
    Microsoft disables SMTP AUTH on many tenants by default; if authentication
    fails with a policy error, either have it enabled for the sending mailbox or
    use :class:`MicrosoftGraphTransport`, which uses the modern API instead.
    """

    def __init__(self, transport: TransportConfig) -> None:
        self._config = transport

    def send(self, message: EmailMessageSpec) -> None:
        cfg = self._config
        password = cfg.resolve_secret()
        if cfg.smtp_auth == "password" and not password:
            raise EmailSendError(
                f"SMTP username {cfg.username!r} is configured but the password "
                f"env var {cfg.password_env or '(unset)'!r} is empty. Set it in "
                f"the environment; it must not go in config/alerting.yml."
            )
        mime = _build_mime(message)
        try:
            context = ssl.create_default_context() if cfg.smtp_tls != "none" else None
            if cfg.smtp_tls == "implicit_tls":
                connection = smtplib.SMTP_SSL(
                    cfg.host, cfg.port, timeout=cfg.timeout_seconds, context=context
                )
            else:
                connection = smtplib.SMTP(
                    cfg.host, cfg.port, timeout=cfg.timeout_seconds
                )
            with connection as smtp:
                smtp.ehlo()
                if cfg.smtp_tls == "starttls":
                    smtp.starttls(context=context)
                    smtp.ehlo()
                if cfg.smtp_auth == "password":
                    smtp.login(cfg.username, password)
                # Envelope recipients include bcc; headers do not.
                refused = smtp.send_message(
                    mime,
                    from_addr=message.from_address,
                    to_addrs=message.all_recipients,
                )
                if refused:
                    raise EmailSendError(
                        f"SMTP server refused {len(refused)} recipient(s); delivery was partial"
                    )
        except EmailSendError:
            raise
        except smtplib.SMTPAuthenticationError:
            raise EmailSendError(
                f"SMTP authentication failed for {cfg.username!r} at {cfg.host}. "
                f"For Microsoft 365 this usually means SMTP AUTH is disabled for "
                f"the mailbox or the account requires modern auth — consider the "
                f"'microsoft_graph' transport."
            ) from None
        except Exception as exc:
            raise EmailSendError(
                f"SMTP send to {cfg.host}:{cfg.port} failed ({type(exc).__name__})"
            ) from None


class MicrosoftGraphTransport:
    """Send as a mailbox via the Microsoft Graph API.

    The modern path for Outlook/Microsoft 365, and the one that works on tenants
    where SMTP AUTH is switched off. Uses the client-credentials flow, so it
    needs an app registration with the ``Mail.Send`` application permission and
    admin consent.
    """

    TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    SEND_URL = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"

    def __init__(self, transport: TransportConfig) -> None:
        self._config = transport

    def _access_token(self, requests_mod: Any) -> str:
        cfg = self._config
        secret = cfg.resolve_secret()
        if not secret:
            raise EmailSendError(
                f"env var {cfg.client_secret_env!r} is empty — Microsoft Graph "
                f"needs the app registration's client secret."
            )
        try:
            response = requests_mod.post(
                self.TOKEN_URL.format(tenant=cfg.tenant_id),
                data={
                    "client_id": cfg.client_id,
                    "client_secret": secret,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
                timeout=cfg.timeout_seconds,
            )
        except Exception as exc:
            raise EmailSendError(
                f"Microsoft Graph token request failed ({type(exc).__name__})"
            ) from None
        if response.status_code >= 300:
            # Deliberately does not echo the body: token endpoints can reflect
            # request parameters, and this string may reach a log.
            raise EmailSendError(
                f"Microsoft Graph token request failed with HTTP {response.status_code}"
            )
        token = response.json().get("access_token", "")
        if not token:
            raise EmailSendError("Microsoft Graph token response had no access_token")
        return token

    def send(self, message: EmailMessageSpec) -> None:
        try:
            import requests
        except ImportError as exc:  # pragma: no cover - requests is a core dep
            raise EmailSendError(
                "requests is required for the Graph transport"
            ) from exc

        token = self._access_token(requests)
        payload = {
            "message": {
                "subject": message.subject,
                "body": {"contentType": "HTML", "content": message.html_body},
                "toRecipients": [{"emailAddress": {"address": a}} for a in message.to],
                "ccRecipients": [{"emailAddress": {"address": a}} for a in message.cc],
                "bccRecipients": [
                    {"emailAddress": {"address": a}} for a in message.bcc
                ],
                "attachments": [
                    {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": name,
                        "contentType": mime,
                        "contentBytes": base64.b64encode(data).decode("ascii"),
                    }
                    for name, data, mime in message.attachments
                ],
            },
            "saveToSentItems": True,
        }
        try:
            response = requests.post(
                self.SEND_URL.format(sender=message.from_address),
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
                timeout=self._config.timeout_seconds,
            )
        except Exception as exc:
            raise EmailSendError(
                f"Microsoft Graph sendMail failed ({type(exc).__name__}); delivery may be unknown"
            ) from None
        if response.status_code >= 300:
            raise EmailSendError(
                f"Microsoft Graph sendMail failed with HTTP {response.status_code}"
            )


class ConsoleEmailTransport:
    """Log the message instead of sending it.

    The default, so that alerting is *visible* before it is *configured*: a
    pipeline with no mail setup still emits the summary where an operator can
    read it, rather than doing nothing and appearing healthy.
    """

    def send(self, message: EmailMessageSpec) -> None:
        log.info(
            "ALERT (console transport, not sent)\nTo: %s\nSubject: %s\n\n%s",
            ", ".join(message.all_recipients) or "(no recipients)",
            message.subject,
            message.text_body,
        )


class FileEmailTransport:
    """Write the rendered message to a directory. Useful in CI and for eyeballing
    the HTML in a browser before pointing this at a real mailbox."""

    def __init__(self, transport: TransportConfig) -> None:
        self._dir = Path(transport.output_dir)

    def send(self, message: EmailMessageSpec) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        stem = "".join(c if c.isalnum() or c in "-_" else "_" for c in message.subject)[
            :80
        ]
        base = self._dir / stem
        try:
            base.with_suffix(".html").write_text(message.html_body, encoding="utf-8")
            base.with_suffix(".txt").write_text(message.text_body, encoding="utf-8")
        except OSError as exc:
            raise EmailSendError(
                f"could not write alert to {self._dir}: {exc}"
            ) from exc
        log.info("ALERT written to %s.{html,txt}", base)


def build_transport(config: AlertingConfig, *, verdict: str = "") -> EmailTransport:
    """Instantiate the transport named in the config."""
    transport = config.profile_for(verdict).transport
    kind = transport.kind
    if kind in ("outlook_smtp", "smtp"):
        return SmtpEmailTransport(transport)
    if kind == "microsoft_graph":
        return MicrosoftGraphTransport(transport)
    if kind == "file":
        return FileEmailTransport(transport)
    return ConsoleEmailTransport()


def outlook_defaults(
    username: str, password_env: str = "FRED_SMTP_PASSWORD"
) -> TransportConfig:
    """A ready-made Microsoft 365 SMTP transport, for callers configuring in
    code rather than YAML."""
    return TransportConfig(
        kind="outlook_smtp",
        host="smtp.office365.com",
        port=587,
        use_starttls=True,
        username=username,
        password_env=password_env,
    )


def redact(value: Optional[str]) -> str:
    """Never log a credential. Used in diagnostics that mention config."""
    if not value:
        return "(unset)"
    return f"{value[:2]}***{value[-1:]}" if len(value) > 4 else "***"


def env_is_configured(transport: TransportConfig) -> bool:
    """Whether the secret this transport needs is actually present."""
    name = transport.client_secret_env or transport.password_env
    return bool(name and os.environ.get(name))
