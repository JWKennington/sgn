"""Utilities for profiling."""

from __future__ import annotations

import linecache
import logging
import tracemalloc
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar

from sgn.logger import SGN_LOG_LEVELS

if TYPE_CHECKING:
    from tracemalloc import Snapshot, Statistic, StatisticDiff

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


SGN_FIRST_MEM_USAGE: float | None = None


def async_sgn_mem_profile(logger) -> Callable[[F], F]:
    """Decorator for async functions to enable memory profiling based on logger level.

    This decorator provides efficient memory profiling by checking the logger level
    only on the first function call, then using the appropriate wrapper (profiling
    or no-op) for all subsequent calls.

    The memory profiling is enabled when the logger's effective level is at or below
    the MEMPROF level (5). When enabled, it uses Python's tracemalloc to capture
    memory snapshots before and after function execution, displaying detailed
    memory usage statistics.

    Args:
        logger: The logger instance to use for determining profiling level and
               outputting memory statistics.

    Returns:
        A decorator function that wraps async functions with memory profiling
        capabilities.

    Example:
        >>> logger = logging.getLogger("my_app")
        >>> @async_sgn_mem_profile(logger)
        ... async def my_function():
        ...     # Function implementation
        ...     pass
    """

    def decorator(func: F) -> F:
        actual_wrapper: Callable[..., Awaitable[Any]] | None = None

        async def profiling_wrapper(*args: Any, **kwargs: Any) -> Any:
            """Memory profiling wrapper that captures and reports memory usage."""
            if not tracemalloc.is_tracing():
                tracemalloc.start()
            snap1 = tracemalloc.take_snapshot()
            result = await func(*args, **kwargs)
            snap2 = tracemalloc.take_snapshot()
            display_top(logger, snap1, snap2)
            return result

        async def no_op_wrapper(*args: Any, **kwargs: Any) -> Any:
            """No-op wrapper that executes the function without profiling overhead."""
            result = await func(*args, **kwargs)
            return result

        async def dynamic_wrapper(*args: Any, **kwargs: Any) -> Any:
            """Wrapper that determines implementation on first call then delegates."""
            nonlocal actual_wrapper

            if actual_wrapper is None:
                # First call - determine which wrapper to use based on logger level
                log_level = logger.getEffectiveLevel()
                if (
                    log_level <= SGN_LOG_LEVELS["MEMPROF"]
                    and log_level != logging.NOTSET
                ):
                    actual_wrapper = profiling_wrapper
                else:
                    actual_wrapper = no_op_wrapper

            # Delegate to the determined wrapper
            return await actual_wrapper(*args, **kwargs)

        return dynamic_wrapper  # type: ignore

    return decorator


def display_topstats(
    logger,
    top_stats: list[Statistic] | list[StatisticDiff],
    limit: int,
    msg: str = "cumulative",
) -> int:
    """Display memory usage statistics in a formatted table.

    Args:
        logger: SGN logger instance with memprofile method for outputting statistics.
        top_stats: List of memory statistics to display.
        limit: Maximum number of top entries to show.
        msg: Description message for the statistics type (e.g., "cumulative", "diff").

    Returns:
        Total memory size in bytes across all statistics.
    """
    logger.memprofile("\n[MEMPROF] | Top %s lines of memory usage: %s", limit, msg)
    logger.memprofile("[MEMPROF] |")
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]  # type: ignore[attr-defined]
        logger.memprofile(
            "[MEMPROF] | #%s: %s:%s: %.1f KiB",
            index,
            frame.filename,
            frame.lineno,
            stat.size / 1024,  # type: ignore[attr-defined]
        )
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            logger.memprofile("[MEMPROF] |     %s", line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        logger.memprofile("[MEMPROF] | %s other: %.1f KiB", len(other), size / 1024)
    total = sum(stat.size for stat in top_stats)
    logger.memprofile("[MEMPROF] | Total allocated size: %.1f KiB", total / 1024)
    return total


def display_top(
    logger,
    snapshot1: Snapshot,
    snapshot2: Snapshot,
    key_type: str = "lineno",
    limit: int = 10,
) -> None:
    """Display comprehensive memory profiling information from two snapshots.

    This function compares two memory snapshots to show both cumulative memory
    usage and the difference between snapshots. It filters out internal Python
    memory allocations and displays the top memory-consuming lines of code.

    Args:
        logger: SGN logger instance with memprofile method for outputting statistics.
        snapshot1: First memory snapshot (taken before the operation).
        snapshot2: Second memory snapshot (taken after the operation).
        key_type: Grouping method for statistics ("lineno", "filename", or "traceback").
        limit: Maximum number of top entries to display in each section.

    Note:
        This function tracks the first memory usage measurement globally and
        shows the change from that baseline in subsequent calls.
    """
    global SGN_FIRST_MEM_USAGE
    snapshot1 = snapshot1.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        )
    )
    snapshot2 = snapshot2.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        )
    )
    top_stats = snapshot1.statistics(key_type)
    diff_stats = snapshot2.compare_to(snapshot1, key_type)

    logger.memprofile("\n[MEMPROF] -------------------------------------------------")

    total = display_topstats(logger, top_stats, limit, "cumulative")
    display_topstats(logger, diff_stats, limit, "diff from previous")

    if SGN_FIRST_MEM_USAGE is None:
        SGN_FIRST_MEM_USAGE = total / 1024
    logger.memprofile(
        "[MEMPROF] | Change from start %.1f KiB", total / 1024 - SGN_FIRST_MEM_USAGE
    )
    logger.memprofile("[MEMPROF] -------------------------------------------------\n")
