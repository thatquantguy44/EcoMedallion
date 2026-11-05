"""Session-level ECB writer exclusion against an isolated PostgreSQL database."""

import os
import subprocess
import sys
import threading
import time

import pytest


@pytest.fixture
def pg_dsn():
    dsn = os.environ.get("TEST_ECB_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_ECB_POSTGRES_DSN is not configured")
    return dsn


@pytest.fixture
def connections(pg_dsn):
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(pg_dsn) as first, psycopg.connect(pg_dsn) as second:
        yield first, second


def test_lock_contends_across_sessions_and_survives_commit(connections):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock

    first, second = connections
    with ECBWriterLock(first) as owner:
        assert owner.acquired
        first.commit()
        with ECBWriterLock(second) as contender:
            assert not contender.acquired
    with ECBWriterLock(second) as successor:
        assert successor.acquired


def test_lock_releases_after_exception_and_aborted_transaction(connections):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock

    first, second = connections
    with pytest.raises(Exception, match="division by zero"):
        with ECBWriterLock(first) as owner:
            assert owner.acquired
            first.execute("SELECT 1 / 0")
    with ECBWriterLock(second) as successor:
        assert successor.acquired


def test_lock_rejects_recursive_acquisition_on_the_same_session(connections):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock

    first, _ = connections
    with ECBWriterLock(first) as owner:
        assert owner.acquired
        with pytest.raises(RuntimeError, match="already held"):
            with ECBWriterLock(first):
                pass


def test_same_session_cannot_be_acquired_concurrently(monkeypatch):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock

    class Connection:
        pass

    connection = Connection()
    owner = ECBWriterLock(connection)
    entered = threading.Event()
    release = threading.Event()

    def delayed_query(_sql, _column):
        entered.set()
        assert release.wait(timeout=5)
        return True

    monkeypatch.setattr(owner, "_query", delayed_query)
    worker = threading.Thread(target=owner.__enter__)
    worker.start()
    try:
        assert entered.wait(timeout=5)
        with pytest.raises(RuntimeError, match="already held"):
            ECBWriterLock(connection).__enter__()
    finally:
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()


def test_lock_does_not_commit_a_borrowed_transaction(connections):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock

    first, _ = connections
    first.execute("CREATE TEMP TABLE lock_transaction_test (value integer)")
    first.execute("INSERT INTO lock_transaction_test VALUES (1)")
    with ECBWriterLock(first) as lock:
        assert lock.acquired
    assert first.info.transaction_status.name == "INTRANS"
    first.rollback()
    assert (
        first.execute("SELECT to_regclass('pg_temp.lock_transaction_test')").fetchone()[
            0
        ]
        is None
    )


def test_cleanup_failure_closes_writing_session(connections):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock

    first, second = connections

    class BrokenUnlock:
        def __init__(self, connection):
            self.inner = connection
            self.info = connection.info

        def cursor(self):
            if getattr(self, "_fred_ecb_writer_lock_held", False):
                raise RuntimeError("injected unlock failure")
            return self.inner.cursor()

        def commit(self):
            self.inner.commit()

        def close(self):
            self.inner.close()

    with pytest.raises(RuntimeError, match="injected unlock failure"):
        with ECBWriterLock(BrokenUnlock(first)) as lock:
            assert lock.acquired
    assert first.closed
    with ECBWriterLock(second) as successor:
        assert successor.acquired


def test_lock_released_by_process_death(pg_dsn):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock

    psycopg = pytest.importorskip("psycopg")
    script = """
import os, sys, psycopg
from fred_pipeline.ecb_writer_lock import ECBWriterLock
with psycopg.connect(os.environ['TEST_ECB_POSTGRES_DSN']) as conn:
    with ECBWriterLock(conn) as lock:
        print('locked' if lock.acquired else 'busy', flush=True)
        sys.stdin.read()
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "locked"
        with psycopg.connect(pg_dsn) as conn:
            with ECBWriterLock(conn) as contender:
                assert not contender.acquired
            child.kill()
            child.wait(timeout=10)
            deadline = time.monotonic() + 5
            while True:
                with ECBWriterLock(conn) as successor:
                    if successor.acquired:
                        break
                assert time.monotonic() < deadline, (
                    "server did not release dead session"
                )
                time.sleep(0.05)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        if child.stdin:
            child.stdin.close()
        if child.stdout:
            child.stdout.close()
        if child.stderr:
            child.stderr.close()
