"""Explicit Microsoft delegated sign-in and private, locked MSAL cache."""

from __future__ import annotations

import contextlib
import os
import stat
import tempfile
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlsplit

from fred_pipeline.governance.alerting_config import TransportConfig
from fred_pipeline.governance.email_transport import EmailSendError

GRAPH_SCOPE = "https://graph.microsoft.com/Mail.Send"
SMTP_SCOPE = "https://outlook.office.com/SMTP.Send"


class _IdentityHttpClient:
    """Keep identity requests on the Microsoft authority and reject redirects."""

    def __init__(self, inner: Any = None) -> None:
        if inner is None:
            import requests

            inner = requests
        self.inner = inner

    def _request(self, method: str, url: str, **kwargs: Any) -> Any:
        parts = urlsplit(url)
        if (
            parts.scheme != "https"
            or parts.hostname != "login.microsoftonline.com"
            or (parts.port or 443) != 443
            or parts.username is not None
            or parts.password is not None
        ):
            raise EmailSendError("OAuth identity request left the Microsoft authority")
        kwargs["allow_redirects"] = False
        return getattr(self.inner, method)(url, **kwargs)

    def get(self, url: str, **kwargs: Any) -> Any:
        return self._request("get", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Any:
        return self._request("post", url, **kwargs)


def _private_file(fd: int) -> None:
    info = os.fstat(fd)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
        or info.st_nlink != 1
    ):
        raise EmailSendError(
            "OAuth cache and lock files must be owned, private regular files"
        )


@contextlib.contextmanager
def _locked_cache(config: TransportConfig) -> Iterator[tuple[Any, Path]]:
    try:
        import fcntl
        import msal
    except ImportError:
        raise EmailSendError(
            "Delegated email OAuth requires POSIX and the optional email-oauth dependency"
        ) from None
    value = os.environ.get(config.token_cache_path_env, "")
    if not value:
        raise EmailSendError(
            f"OAuth cache path environment variable {config.token_cache_path_env!r} is empty"
        )
    path = Path(value).expanduser()
    if not path.is_absolute() or path.name in ("", ".", ".."):
        raise EmailSendError("OAuth cache path must be absolute")
    parent = path.parent
    try:
        if parent.is_symlink() or not parent.is_dir():
            raise EmailSendError("OAuth cache requires an existing private directory")
        directory = parent.stat()
        if directory.st_uid != os.getuid() or stat.S_IMODE(directory.st_mode) & 0o077:
            raise EmailSendError(
                "OAuth cache directory must be owned and private (0700)"
            )
        repo_root = Path(__file__).resolve().parents[3]
        if parent.resolve().is_relative_to(repo_root):
            raise EmailSendError("OAuth cache must be outside the repository")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        lock_fd = os.open(str(path) + ".lock", flags, 0o600)
        try:
            _private_file(lock_fd)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            cache = msal.SerializableTokenCache()
            try:
                cache_fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            except FileNotFoundError:
                cache_fd = None
            if cache_fd is not None:
                try:
                    _private_file(cache_fd)
                    with os.fdopen(
                        cache_fd, "r", encoding="utf-8", closefd=False
                    ) as handle:
                        cache.deserialize(handle.read())
                finally:
                    os.close(cache_fd)
            try:
                yield cache, path
            finally:
                if cache.has_state_changed:
                    fd, temporary = tempfile.mkstemp(prefix=".email-oauth-", dir=parent)
                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as handle:
                            handle.write(cache.serialize())
                            handle.flush()
                            os.fsync(handle.fileno())
                        os.replace(temporary, path)
                    finally:
                        if os.path.exists(temporary):
                            os.unlink(temporary)
        finally:
            os.close(lock_fd)
    except EmailSendError:
        raise
    except Exception as exc:
        raise EmailSendError(
            f"OAuth cache or authentication failed ({type(exc).__name__})"
        ) from None


def delegated_token(
    config: TransportConfig,
    sender: str,
    *,
    http_client: Any = None,
    interactive: bool = False,
) -> str:
    """Acquire one audience-specific token; only explicit bootstrap may prompt."""
    scope = GRAPH_SCOPE if config.kind == "microsoft_graph" else SMTP_SCOPE
    with _locked_cache(config) as (cache, _path):
        import msal

        options: dict[str, Any] = {
            "authority": f"https://login.microsoftonline.com/{config.tenant_id}",
            "token_cache": cache,
        }
        options["http_client"] = _IdentityHttpClient(http_client)
        app = msal.PublicClientApplication(config.client_id, **options)
        if interactive:
            result = app.acquire_token_interactive(
                scopes=[scope], login_hint=sender, timeout=300
            )
        else:
            accounts = [
                a
                for a in app.get_accounts(username=sender)
                if a.get("username", "").casefold() == sender.casefold()
            ]
            if len(accounts) != 1:
                raise EmailSendError(
                    "OAuth account does not match the configured sender; run alerts auth"
                )
            result = app.acquire_token_silent(scopes=[scope], account=accounts[0])
        accounts = [
            a
            for a in app.get_accounts(username=sender)
            if a.get("username", "").casefold() == sender.casefold()
        ]
        if len(accounts) != 1:
            raise EmailSendError(
                "OAuth account does not match the configured sender; run alerts auth"
            )
        if not result or not result.get("access_token"):
            raise EmailSendError(
                "OAuth authorization is unavailable; run alerts auth to sign in again"
            )
        return result["access_token"]


def check_cached_account(config: TransportConfig, sender: str) -> None:
    """Check private cache metadata without refreshing or contacting Microsoft."""
    with _locked_cache(config) as (cache, _path):
        accounts = [
            entry
            for entry in cache.search("Account")
            if entry.get("username", "").casefold() == sender.casefold()
        ]
        if len(accounts) != 1:
            raise EmailSendError(
                "OAuth cache account does not match the configured sender; run alerts auth"
            )
