"""Decorator for timing a call and logging its elapsed wall-clock time.

Avoids hand-rolled ``time.perf_counter()`` bookkeeping at each call site.
Logs through the standard :mod:`logging` module, so output is captured by
whatever handler/level ``cli.py``'s ``logging.basicConfig`` (or the
Databricks job's logging setup) already configures.
"""

from __future__ import annotations

import functools
import inspect
import logging
import time
from contextlib import contextmanager, nullcontext
from collections.abc import Callable
from typing import Any, TypeVar

from fred_pipeline.performance import Measurement, span

F = TypeVar("F", bound=Callable[..., Any])


def timed(
    name: str | None = None,
    *,
    logger: logging.Logger | None = None,
    progress_callback: Callable[[str, float], None] | None = None,
    log_completion: bool = True,
    attributes: Callable[..., dict[str, Any]] | None = None,
    count_result: str | None = None,
    collect: bool = True,
) -> Callable[[F], F]:
    """Log the wrapped callable's elapsed wall-clock time on return or raise.

    ``name`` overrides the logged label (defaults to the function's
    ``__qualname__``, e.g. ``FredPipeline.run``). ``logger`` overrides the
    logger used (defaults to one named after the wrapped function's module).
    ``progress_callback``, if provided, is called with (label, elapsed_seconds)
    when the wrapped function completes successfully (typically for progress
    bar updates).
    """

    def decorator(func: F) -> F:
        label = name or func.__qualname__
        log = logger or logging.getLogger(func.__module__)

        @contextmanager
        def measure(args: Any, kwargs: Any):
            tags = {}
            if attributes is not None:
                try:
                    tags = attributes(*args, **kwargs)
                except Exception:
                    log.warning("Timing attributes unavailable for %s", label)
            start = time.perf_counter()
            try:
                with (
                    span(label, **tags)
                    if collect
                    else nullcontext(Measurement(name=label))
                ) as measurement:
                    yield measurement
            except BaseException:
                if log_completion:
                    log.error(
                        "%s failed after %.2fs", label, time.perf_counter() - start
                    )
                raise

        def completed(start: float) -> None:
            elapsed = time.perf_counter() - start
            if log_completion:
                log.info("%s finished in %.2fs", label, elapsed)
            if progress_callback:
                try:
                    progress_callback(label, elapsed)
                except Exception:
                    log.warning("Timing progress callback failed for %s", label)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            with measure(args, kwargs) as measurement:
                result = func(*args, **kwargs)
                if count_result and isinstance(result, (int, list, tuple)):
                    measurement.count(
                        count_result, result if isinstance(result, int) else len(result)
                    )
            completed(start)
            return result

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter()
                with measure(args, kwargs) as measurement:
                    result = await func(*args, **kwargs)
                    if count_result and isinstance(result, (int, list, tuple)):
                        measurement.count(
                            count_result,
                            result if isinstance(result, int) else len(result),
                        )
                completed(start)
                return result

            return async_wrapper  # type: ignore[return-value]
        return wrapper  # type: ignore[return-value]

    return decorator
