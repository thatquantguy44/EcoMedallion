"""Run-scoped, opt-in HTTP routing for main-pipeline data sources."""

from __future__ import annotations

import ipaddress
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import requests


class NetworkConfigError(ValueError):
    """Invalid managed-network configuration."""


class NetworkPreflightError(RuntimeError):
    """No permitted route passed the startup check."""


class _PolicyError(Exception):
    pass


def _origin(url: str) -> tuple[str, str, int]:
    try:
        parts = urlsplit(url)
        port = parts.port
        if parts.scheme not in {"http", "https"} or not parts.hostname or port == 0:
            raise ValueError
        if parts.username or parts.password:
            raise ValueError
        host = parts.hostname.lower().rstrip(".")
        try:
            host = str(ipaddress.ip_address(host))
        except ValueError:
            pass
        return (
            parts.scheme,
            host,
            port or (443 if parts.scheme == "https" else 80),
        )
    except ValueError:
        raise NetworkConfigError("invalid HTTP destination URL") from None


def _proxy_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError
    except ValueError:
        raise NetworkConfigError("proxy must be a valid HTTP(S) proxy URL") from None
    return value


def _positive(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise NetworkConfigError(f"network.{name} must be positive")
    try:
        result = float(value)
    except (ValueError, TypeError):
        raise NetworkConfigError(f"network.{name} must be positive") from None
    if not 0 < result < float("inf"):
        raise NetworkConfigError(f"network.{name} must be positive")
    return result


def _bypass_item(value: str) -> tuple[str, int | None]:
    if not isinstance(value, str) or not value.strip() or "/" in value:
        raise NetworkConfigError("network.no_proxy contains an invalid host")
    value = value.strip().lower()
    if value == "*":
        return value, None
    if value.startswith("["):
        try:
            parsed = urlsplit(f"http://{value}")
            ipaddress.IPv6Address(parsed.hostname or "")
            port = parsed.port
            if port == 0:
                raise ValueError
        except ValueError:
            raise NetworkConfigError(
                "network.no_proxy contains an invalid IPv6 host"
            ) from None
        return str(ipaddress.IPv6Address(parsed.hostname)), port
    try:
        return str(ipaddress.ip_address(value)), None
    except ValueError:
        pass
    host, sep, port_text = value.rpartition(":")
    if sep:
        if not port_text.isdigit() or not host or not 1 <= int(port_text) <= 65535:
            raise NetworkConfigError("network.no_proxy contains an invalid port")
        value, port = host, int(port_text)
    else:
        port = None
    host = value.lstrip(".").rstrip(".")
    if not host or any(
        not (part and part.replace("-", "").isalnum()) for part in host.split(".")
    ):
        raise NetworkConfigError("network.no_proxy contains an invalid host")
    return value.rstrip("."), port


@dataclass(frozen=True)
class NetworkConfig:
    mode: str = "env"
    preference: str = "proxy_first"
    inherit_proxy_env: bool = True
    http_proxy_env: str = ""
    https_proxy_env: str = ""
    no_proxy: tuple[str, ...] = ()
    ca_bundle_env: str = ""
    connect_timeout_seconds: float = 3
    read_timeout_seconds: float = 3
    max_parallel_targets: int = 4
    origins: tuple[tuple[tuple[str, str, int], str], ...] = ()

    @classmethod
    def from_mapping(cls, raw: Any) -> NetworkConfig:
        if raw is None:
            return cls()
        if isinstance(raw, cls):
            return raw
        if not isinstance(raw, dict):
            raise NetworkConfigError("network must be a mapping")
        allowed = {
            "mode",
            "preference",
            "inherit_proxy_env",
            "http_proxy_env",
            "https_proxy_env",
            "no_proxy",
            "ca_bundle_env",
            "probe",
            "origins",
        }
        unknown = set(raw) - allowed
        if unknown:
            raise NetworkConfigError(f"unknown network settings: {sorted(unknown)}")
        mode = raw.get("mode", "env")
        preference = raw.get("preference", "proxy_first")
        if not isinstance(mode, str) or mode not in {"env", "direct", "proxy", "auto"}:
            raise NetworkConfigError("network.mode must be env, direct, proxy or auto")
        if not isinstance(preference, str) or preference not in {
            "proxy_first",
            "direct_first",
        }:
            raise NetworkConfigError(
                "network.preference must be proxy_first or direct_first"
            )
        inherit = raw.get("inherit_proxy_env", True)
        if not isinstance(inherit, bool):
            raise NetworkConfigError("network.inherit_proxy_env must be a boolean")
        names = {}
        for key in ("http_proxy_env", "https_proxy_env", "ca_bundle_env"):
            name = raw.get(key, "")
            if not isinstance(name, str) or (name and not name.isidentifier()):
                raise NetworkConfigError(
                    f"network.{key} must name an environment variable"
                )
            names[key] = name
        bypass = raw.get("no_proxy", [])
        if not isinstance(bypass, (tuple, list)):
            raise NetworkConfigError("network.no_proxy must be a list")
        for entry in bypass:
            _bypass_item(entry)
        probe = raw.get("probe", {})
        if not isinstance(probe, dict) or set(probe) - {
            "connect_timeout_seconds",
            "read_timeout_seconds",
            "max_parallel_targets",
        }:
            raise NetworkConfigError(
                "network.probe contains unknown or invalid settings"
            )
        if "max_parallel_targets" in probe:
            count = probe["max_parallel_targets"]
            if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                raise NetworkConfigError(
                    "network.probe.max_parallel_targets must be positive"
                )
        origins = raw.get("origins", {})
        if not isinstance(origins, dict):
            raise NetworkConfigError("network.origins must be a mapping")
        overrides = []
        for url, options in origins.items():
            if (
                not isinstance(url, str)
                or not isinstance(options, dict)
                or set(options) != {"mode"}
            ):
                raise NetworkConfigError("network origin overrides require only a mode")
            parsed = urlsplit(url)
            if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
                raise NetworkConfigError(
                    "network origin overrides must not include a path"
                )
            if not isinstance(options["mode"], str) or options["mode"] not in {
                "direct",
                "proxy",
                "auto",
            }:
                raise NetworkConfigError(
                    "network origin mode must be direct, proxy or auto"
                )
            overrides.append((_origin(url), options["mode"]))
        if mode == "env" and (
            names["http_proxy_env"] or names["https_proxy_env"] or overrides
        ):
            raise NetworkConfigError(
                "managed proxy/origin overrides require a managed network mode"
            )
        return cls(
            mode=mode,
            preference=preference,
            inherit_proxy_env=inherit,
            no_proxy=tuple(bypass),
            origins=tuple(overrides),
            **names,
            connect_timeout_seconds=_positive(
                probe.get("connect_timeout_seconds", 3), "probe.connect_timeout_seconds"
            ),
            read_timeout_seconds=_positive(
                probe.get("read_timeout_seconds", 3), "probe.read_timeout_seconds"
            ),
            max_parallel_targets=probe.get("max_parallel_targets", 4),
        )


@dataclass(frozen=True)
class Route:
    kind: str
    origin: tuple[str, str, int]
    proxy_url: str = field(default="", repr=False)
    ca_bundle: str | bool = field(default=True, repr=False)
    probe_headers: dict[str, str] = field(
        default_factory=dict, repr=False, compare=False
    )

    @property
    def proxies(self) -> dict[str, str]:
        return {self.origin[0]: self.proxy_url} if self.kind == "proxy" else {}


def _env(lower: str, environ: Mapping[str, str]) -> str:
    if lower in environ:
        return environ[lower]
    if lower == "http_proxy" and "REQUEST_METHOD" in environ:
        return ""
    return environ.get(lower.upper(), "")


def _matches_bypass(origin: tuple[str, str, int], values: list[str]) -> bool:
    _, host, port = origin
    for value in values:
        pattern, target_port = _bypass_item(value)
        if target_port is not None and target_port != port:
            continue
        if pattern == "*" or pattern == host:
            return True
        if pattern.startswith(".") and (host == pattern[1:] or host.endswith(pattern)):
            return True
    return False


def _probe(url: str, route: Route, settings: NetworkConfig) -> None:
    with requests.Session() as session:
        session.trust_env = False
        with session.head(
            url,
            proxies=route.proxies,
            verify=route.ca_bundle,
            allow_redirects=False,
            headers=route.probe_headers,
            timeout=(settings.connect_timeout_seconds, settings.read_timeout_seconds),
        ) as response:
            if route.kind == "proxy" and (
                response.status_code == 407
                or (
                    route.origin[0] == "http"
                    and response.status_code >= 500
                    and (
                        response.headers.get("Via")
                        or response.headers.get("Proxy-Status")
                    )
                )
            ):
                raise _PolicyError("proxy refused probe")


def select_route(
    settings: NetworkConfig,
    url: str,
    *,
    label: str = "FRED",
    headers: dict[str, str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Route:
    """Probe at most two permitted routes without source credentials."""
    environ = dict(os.environ) if environ is None else environ
    target = _origin(url)
    mode = dict(settings.origins).get(target, settings.mode)
    if mode == "env":
        raise NetworkConfigError("env mode does not select managed routes")
    bypass = list(settings.no_proxy)
    if "FRED_NO_PROXY" in environ:
        bypass = [
            item.strip() for item in environ["FRED_NO_PROXY"].split(",") if item.strip()
        ]
    if settings.inherit_proxy_env:
        bypass.extend(
            item.strip()
            for item in _env("no_proxy", environ).split(",")
            if item.strip()
        )
    if _matches_bypass(target, bypass):
        mode = "direct"
    proxy = ""
    if mode != "direct":
        named = (
            settings.https_proxy_env
            if target[0] == "https"
            else settings.http_proxy_env
        )
        app_var = "FRED_HTTPS_PROXY" if target[0] == "https" else "FRED_HTTP_PROXY"
        proxy = environ.get(app_var, "") or (environ.get(named, "") if named else "")
        if not proxy and settings.inherit_proxy_env:
            proxy = _env(f"{target[0]}_proxy", environ) or _env("all_proxy", environ)
    if proxy:
        proxy = _proxy_url(proxy)
    ca_bundle = (
        environ.get("FRED_CA_BUNDLE")
        or (environ.get(settings.ca_bundle_env, "") if settings.ca_bundle_env else "")
        or environ.get("REQUESTS_CA_BUNDLE")
        or environ.get("CURL_CA_BUNDLE")
        or True
    )
    candidates = (
        [mode]
        if mode in {"direct", "proxy"}
        else (
            ["proxy", "direct"]
            if settings.preference == "proxy_first"
            else ["direct", "proxy"]
        )
    )
    if mode == "proxy" and not proxy:
        raise NetworkPreflightError(
            f"{label} proxy route is required but no proxy is configured"
        )
    for kind in candidates:
        if kind == "proxy" and not proxy:
            continue
        route = Route(
            kind, target, proxy if kind == "proxy" else "", ca_bundle, headers or {}
        )
        try:
            probe_url = urlsplit(url)._replace(query="", fragment="").geturl()
            _probe(probe_url, route, settings)
        except (requests.exceptions.SSLError, _PolicyError):
            raise NetworkPreflightError(
                f"{label} {kind} route failed TLS or proxy policy validation"
            ) from None
        except requests.exceptions.ProxyError as exc:
            message = str(exc).lower()
            if any(
                text in message for text in ("407", "403", "tunnel connection failed")
            ):
                raise NetworkPreflightError(
                    f"{label} proxy route failed authentication or CONNECT policy"
                ) from None
            continue
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            continue
        except requests.exceptions.RequestException:
            raise NetworkPreflightError(f"{label} {kind} route probe failed") from None
        return route
    raise NetworkPreflightError(
        f"no permitted {label} route passed the startup connectivity check"
    )


class _OriginSession(requests.Session):
    def __init__(self, routes: dict[tuple[str, str, int], Route]):
        super().__init__()
        self.trust_env = False
        self._routes = routes

    def send(self, request, **kwargs):
        route = self._routes.get(_origin(request.url))
        if route is None:
            raise requests.exceptions.RequestException(
                "request or redirect left the registered origins"
            )
        # Redirect preparation may have carried a different proxy's credentials.
        # The adapter adds authentication for this route's proxy when needed.
        request.headers.pop("Proxy-Authorization", None)
        kwargs["proxies"] = route.proxies
        kwargs["verify"] = route.ca_bundle
        try:
            return super().send(request, **kwargs)
        except requests.exceptions.RequestException:
            raise requests.exceptions.RequestException(
                "request failed on the selected route"
            ) from None


class RunSessions:
    """One configured Requests session per thread; close all at run end."""

    def __init__(self, routes: Route | dict[tuple[str, str, int], Route]):
        self._routes = (
            {routes.origin: routes} if isinstance(routes, Route) else dict(routes)
        )
        self._local = threading.local()
        self._lock = threading.Lock()
        self._sessions: list[_OriginSession] = []

    def _session(self) -> _OriginSession:
        session = getattr(self._local, "session", None)
        if session is None:
            session = _OriginSession(self._routes)
            with self._lock:
                self._sessions.append(session)
            self._local.session = session
        return session

    def get(self, url, **kwargs):
        return self._session().get(url, **kwargs)

    def post(self, url, **kwargs):
        return self._session().post(url, **kwargs)

    def close(self) -> None:
        for session in self._sessions:
            session.close()
