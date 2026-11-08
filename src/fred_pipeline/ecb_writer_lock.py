"""PostgreSQL session lock for the ECB eligible-assets live dataset.

This primitive does not by itself make an entry point safe: callers must use
the same connection for writes and acquire it before setup or source access.
"""

from __future__ import annotations

from threading import Lock
from typing import Any

# ASCII "ECBASSET", represented as a signed-64-bit-compatible integer.
ECB_WRITER_LOCK_KEY = 0x4543424153534554
_HELD_ATTR = "_fred_ecb_writer_lock_held"
_SESSION_GUARD = Lock()


class ECBWriterLock:
    """Hold a nonblocking, exclusive PostgreSQL advisory session lock."""

    def __init__(self, connection: Any):
        self.connection = connection
        self.acquired = False

    def _query(self, sql: str, column: str) -> bool:
        conn = self.connection
        was_idle = conn.info.transaction_status.name == "IDLE"
        with conn.cursor() as cursor:
            cursor.execute(sql, (ECB_WRITER_LOCK_KEY,))
            row = cursor.fetchone()
        result = row[column] if isinstance(row, dict) else row[0]
        # Finish only the transaction started by this lock query. Never commit
        # a transaction supplied by the caller.
        if was_idle:
            conn.commit()
        return bool(result)

    def __enter__(self) -> ECBWriterLock:
        with _SESSION_GUARD:
            if getattr(self.connection, _HELD_ATTR, False):
                raise RuntimeError("ECB writer lock is already held on this session")
            # Reserve before sending SQL: PostgreSQL recursively grants a lock
            # to the same session, including concurrent callers in one process.
            setattr(self.connection, _HELD_ATTR, True)
        try:
            self.acquired = self._query(
                "SELECT pg_try_advisory_lock(%s) AS acquired", "acquired"
            )
        except BaseException:
            # The server may have acquired the lock before the response failed.
            try:
                self.connection.close()
            finally:
                with _SESSION_GUARD:
                    setattr(self.connection, _HELD_ATTR, False)
            raise
        if not self.acquired:
            with _SESSION_GUARD:
                setattr(self.connection, _HELD_ATTR, False)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if not self.acquired:
            return
        try:
            if self.connection.info.transaction_status.name == "INERROR":
                self.connection.rollback()
            if not self._query("SELECT pg_advisory_unlock(%s) AS released", "released"):
                raise RuntimeError("ECB writer lock was not held during cleanup")
        except BaseException:
            # Closing the writing session releases any uncertain lock state.
            self.connection.close()
            raise
        finally:
            with _SESSION_GUARD:
                setattr(self.connection, _HELD_ATTR, False)
            self.acquired = False
