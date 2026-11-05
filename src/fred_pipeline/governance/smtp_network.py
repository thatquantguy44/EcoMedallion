"""Select and reuse direct or HTTP CONNECT SMTP routes."""

from __future__ import annotations

import base64
import http.client
import os
import smtplib
import socket
import ssl
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote, urlsplit

from fred_pipeline.network import _bypass_item, _matches_bypass, _proxy_url


class SmtpProxyPolicyError(Exception):
    """A proxy explicitly rejected the CONNECT request."""


@dataclass(frozen=True)
class SmtpNetworkConfig:
    mode: str = "direct"
    preference: str = "proxy_first"
    proxy_url_env: str = ""
    no_proxy: tuple[str, ...] = ()
    ca_bundle_env: str = ""

    @classmethod
    def from_mapping(cls, raw: Any) -> SmtpNetworkConfig:
        if not isinstance(raw, dict) or set(raw) - {
            "mode",
            "preference",
            "proxy_url_env",
            "no_proxy",
            "ca_bundle_env",
        }:
            raise ValueError("SMTP network contains unknown or invalid settings")
        mode, preference = (
            raw.get("mode", "direct"),
            raw.get("preference", "proxy_first"),
        )
        if (
            not isinstance(mode, str)
            or not isinstance(preference, str)
            or mode not in {"direct", "proxy", "auto"}
            or preference
            not in {
                "proxy_first",
                "direct_first",
            }
        ):
            raise ValueError("SMTP network mode or preference is invalid")
        for name in ("proxy_url_env", "ca_bundle_env"):
            value = raw.get(name, "")
            if not isinstance(value, str) or (value and not value.isidentifier()):
                raise ValueError(
                    f"SMTP network {name} must name an environment variable"
                )
        bypass = raw.get("no_proxy", [])
        if not isinstance(bypass, list):
            raise ValueError("SMTP network no_proxy must be a list")
        for item in bypass:
            _bypass_item(item)
        return cls(
            mode=mode,
            preference=preference,
            proxy_url_env=raw.get("proxy_url_env", ""),
            no_proxy=tuple(bypass),
            ca_bundle_env=raw.get("ca_bundle_env", ""),
        )


@dataclass(frozen=True)
class SmtpRoute:
    kind: str
    proxy_url: str = field(default="", repr=False)
    ca_bundle: str = ""


def _tunnel(proxy_url: str, host: str, port: int, timeout: float) -> socket.socket:
    proxy = urlsplit(proxy_url)
    headers = {}
    if proxy.username is not None:
        credential = (
            f"{unquote(proxy.username)}:{unquote(proxy.password or '')}".encode()
        )
        headers["Proxy-Authorization"] = "Basic " + base64.b64encode(credential).decode(
            "ascii"
        )
    connection = http.client.HTTPConnection(
        proxy.hostname, proxy.port or 80, timeout=timeout
    )
    connection.set_tunnel(host, port, headers=headers)
    try:
        connection.connect()
        sock = connection.sock
        connection.sock = None
        if sock is None:
            raise OSError("proxy did not establish a connection")
        return sock
    except OSError as exc:
        connection.close()
        if "tunnel connection failed" in str(exc).lower():
            raise SmtpProxyPolicyError("proxy rejected CONNECT") from None
        raise


def smtp_connection(config: Any, route: SmtpRoute) -> smtplib.SMTP:
    context = (
        ssl.create_default_context(cafile=route.ca_bundle or None)
        if config.smtp_tls != "none"
        else None
    )
    if route.kind == "direct":
        if config.smtp_tls == "implicit_tls":
            return smtplib.SMTP_SSL(
                config.host,
                config.port,
                timeout=config.timeout_seconds,
                context=context,
            )
        return smtplib.SMTP(config.host, config.port, timeout=config.timeout_seconds)

    class TunnelSMTP(smtplib.SMTP):
        def _get_socket(self, host, port, timeout):
            sock = _tunnel(route.proxy_url, host, port, timeout)
            return (
                context.wrap_socket(sock, server_hostname=host)
                if config.smtp_tls == "implicit_tls"
                else sock
            )

    return TunnelSMTP(config.host, config.port, timeout=config.timeout_seconds)


def smtp_hello(connection: smtplib.SMTP, config: Any, route: SmtpRoute) -> None:
    result = connection.ehlo()
    if result is not None and result[0] >= 400:
        raise smtplib.SMTPHeloError(result[0], b"EHLO failed")
    if config.smtp_tls == "starttls":
        context = ssl.create_default_context(cafile=route.ca_bundle or None)
        connection.starttls(context=context)
        result = connection.ehlo()
        if result is not None and result[0] >= 400:
            raise smtplib.SMTPHeloError(result[0], b"EHLO after STARTTLS failed")


def plan_smtp_route(config: Any) -> SmtpRoute:
    from fred_pipeline.governance.email_transport import EmailSendError

    settings = config.network
    if settings is None:
        return SmtpRoute("direct")
    mode = settings.mode
    if _matches_bypass(
        ("smtp", config.host.rstrip(".").lower(), config.port), list(settings.no_proxy)
    ):
        mode = "direct"
    proxy = (
        os.environ.get(settings.proxy_url_env, "")
        if settings.proxy_url_env and mode != "direct"
        else ""
    )
    if proxy:
        try:
            _proxy_url(proxy)
            if urlsplit(proxy).scheme != "http":
                raise ValueError
        except ValueError:
            raise EmailSendError("SMTP proxy must be an http:// proxy URL") from None
    ca_bundle = (
        os.environ.get(settings.ca_bundle_env, "") if settings.ca_bundle_env else ""
    )
    if settings.ca_bundle_env and not ca_bundle:
        raise EmailSendError(
            f"SMTP CA bundle environment variable {settings.ca_bundle_env!r} is empty"
        )
    if ca_bundle and config.smtp_tls != "none":
        try:
            ssl.create_default_context(cafile=ca_bundle)
        except (OSError, ssl.SSLError):
            raise EmailSendError("SMTP CA bundle is unavailable or invalid") from None
    if mode == "proxy" and not proxy:
        raise EmailSendError("SMTP proxy route is required but no proxy is configured")
    candidates = (
        [mode]
        if mode != "auto"
        else (
            ["proxy", "direct"]
            if settings.preference == "proxy_first"
            else ["direct", "proxy"]
        )
    )
    for kind in candidates:
        if kind == "proxy" and not proxy:
            continue
        route = SmtpRoute(kind, proxy if kind == "proxy" else "", ca_bundle)
        try:
            with smtp_connection(config, route) as connection:
                smtp_hello(connection, config, route)
            return route
        except smtplib.SMTPServerDisconnected:
            continue
        except (ssl.SSLError, SmtpProxyPolicyError, smtplib.SMTPException):
            raise EmailSendError(
                "SMTP route failed TLS, server or proxy policy validation"
            ) from None
        except (OSError, TimeoutError):
            continue
    raise EmailSendError("no permitted SMTP route passed the startup preflight")
