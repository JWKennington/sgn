"""Utilities for profiling."""

from __future__ import annotations

import linecache
import tracemalloc

from sgn.base import SGN_LOG_LEVELS

# from typing import Any, Callable, Dict, Optional, Sequence, Union


def async_sgn_mem_profile(logger):
    def __sgn_mem_profile(func):

        if not tracemalloc.is_tracing():
            tracemalloc.start()

        async def wrapper(*args, **kwargs):
            snap1 = tracemalloc.take_snapshot()
            result = await func(*args, **kwargs)  # type: ignore
            snap2 = tracemalloc.take_snapshot()
            display_top(snap1, snap2, logger)
            return result

        return wrapper

    def __do_nothing(func):
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)  # type: ignore
            return result

        return wrapper

    if logger.level == SGN_LOG_LEVELS["MEMPROF"]:
        return __sgn_mem_profile
    else:
        return __do_nothing


SGN_FIRST_MEM_USAGE = None


def display_topstats(logger, top_stats, limit, msg="cumulative"):
    logger.memprofile("\n[MEMPROF] | Top %s lines of memory usage: %s", limit, msg)
    logger.memprofile("[MEMPROF] |")
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        logger.memprofile(
            "[MEMPROF] | #%s: %s:%s: %.1f KiB",
            index,
            frame.filename,
            frame.lineno,
            stat.size / 1024,
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


def display_top(snapshot1, snapshot2, logger, key_type="lineno", limit=10):
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
