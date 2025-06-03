"""Source elements for generating data streams.

New classes need not be subclassed from DequeSource, but should at least be ultimately a
subclass of SourceElement.
"""

from __future__ import annotations

import os
import signal
import sys
import time
import warnings
from collections import deque
from dataclasses import dataclass
from time import sleep
from typing import Any, Callable, Dict, Generator, Iterable, Iterator, Optional, Union

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from sgn.base import SourceElement, SourcePad
from sgn.frames import Frame


def _handler(signum, frame):
    SignalEOS.rcvd_signals.add(signum)


class SignalEOS:
    """
    This class provides global signal handling for an SGN pipeline.  If you
    inherit it for a source element then it will capture SIGINT and provide a
    method to mark that eos should be flagged.  See NullSource as an example.

    Additionally this must be used as a context manager for executing
    a pipeline and disabling the signal hander after the pipeline is done, e.g.,

        with SignalEOS() as signal_eos:
            p.run()

    """

    handled_signals = {signal.SIGINT, signal.SIGTERM}
    rcvd_signals: set[int] = set([])
    previous_handlers: dict[int, Callable] = {}

    @classmethod
    def signaled_eos(cls):
        """Indicate whether a signal has been received to indicate an EOS.

        Returns true of the intersection of received signals and handled
        signals is nonzero.  This can be used by developers to decide if EOS
        should be set.
        """
        return bool(cls.rcvd_signals & cls.handled_signals)

    def raise_signal(self, sig):
        """Raise a signal that has already been raised previously.

        Intended to be used if the application needs to re-raise one of the
        signals with the previous signal handler.  NOTE - this will only raise
        the signal if it had been previously raised and only within a given
        context.
        """
        if sig in SignalEOS.rcvd_signals:
            signal.raise_signal(sig)

    def __enter__(self):
        """Store the previous signal handlers and setup new ones for the
        handled signals"""
        for sig in SignalEOS.handled_signals:
            SignalEOS.previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore the original signal handlers"""
        for sig in SignalEOS.handled_signals:
            signal.signal(sig, SignalEOS.previous_handlers[sig])
        SignalEOS.rcvd_signals = set([])


@dataclass
class NullSource(SourceElement, SignalEOS):
    """A source that does precisely nothing.

    It is useful for testing and debugging, and will always produce empty frames

        frame_factory: Callable = Frame
        wait: float = None
        num_frames: int = None

    If wait is not None the source will block for wait seconds before each new
    buffer, which is useful for slowing down debugging pipelines.  By default
    this source element handles SIGINT and uses that to set EOS. See SignalEOS.
    In order to use this feature, the pipeline must be run within the SignalEOS
    context manager, e.g.,

        with SignalEOS() as signal_eos:
            p.run()
    """

    frame_factory: Callable = Frame
    wait: Optional[float] = None
    num_frames: Optional[int] = None

    def __post_init__(self):
        super().__post_init__()
        self.frame_count = 0

    def new(self, pad: SourcePad) -> Frame:
        """New Frames are created on "pad" with an instance specific count and a name
        derived from the pad name. EOS is set if we have surpassed the requested number
        of Frames.

        Args:
            pad:
                SourcePad, the pad for which to produce a new Frame

        Returns:
            Frame, the Frame with optional data payload
        """
        if self.wait is not None:
            sleep(self.wait)
        self.frame_count += 1
        return self.frame_factory(
            EOS=self.signaled_eos()
            or (self.num_frames is not None and self.frame_count > self.num_frames),
            data=None,
        )


@dataclass
class IterSource(SourceElement):
    """A source element that has one iterable per source pad.

    The end of stream is controlled by setting an optional limit on the number
    of times a deque can be empty before EOS is signaled.

    Args:
        iters:
            dict[str, Iterable[Any]], a mapping of source pads to iterables,
            where the key is the pad name and the value is the Iterable. These
            will be coerced to iterators, so they can be any iterable type.
        eos_on_empty:
            Union[dict[str, bool], bool], default True, a mapping of source
            pads to boolean values, where the key is the pad name and the value
            is the boolean. If a bool is given, the value is applied to all
            pads. If True, EOS is signaled when the iterator is empty.
    """

    iters: Optional[dict[str, Iterable[Any]]] = None
    eos_on_empty: Union[dict[str, bool], bool] = True
    frame_factory: Callable = Frame

    def __post_init__(self):
        """Post init checks for the DequeSource element."""
        super().__post_init__()
        # Setup pad counts
        self._setup_iters()
        self._setup_eos_on_empty()
        self._validate_iters()
        self._validate_eos_on_empty()

    def _setup_iters(self):
        # Setup the iter_map if not given
        if self.iters is None:
            self.iters = {
                pad.name: self._coerce_iterator([]) for pad in self.source_pads
            }
        else:
            self.iters = {
                name: self._coerce_iterator(iterable)
                for name, iterable in self.iters.items()
            }

    def _setup_eos_on_empty(self):
        # Setup the limits if not given
        if isinstance(self.eos_on_empty, bool):
            self.eos_on_empty = {
                pad.name: self.eos_on_empty for pad in self.source_pads
            }

    def _validate_iters(self):
        # Check that the deque_map has the correct number of deque s
        if not len(self.iters) == len(self.source_pads):
            raise ValueError("The number of deque s must match the number of pads")

        # Check that the deque_map has the correct pad names
        for pad_name in self.iters:
            if pad_name not in [pad.name for pad in self.source_pads]:
                raise ValueError(
                    "DequeSource has a deque for a pad that does not exist, "
                    f"got: {pad_name}, options are: {self.source_pad_names}"
                )

    def _validate_eos_on_empty(self):
        # Check that the limits has the correct number of limits
        if not len(self.eos_on_empty) == len(self.source_pads):
            raise ValueError("The number of eos on empty must match the number of pads")

        # Check that the limits has the correct pad names
        for pad_name in self.eos_on_empty:
            if pad_name not in [pad.name for pad in self.source_pads]:
                raise ValueError(
                    f"DequeSource has a eos on empty for a pad that does not exist, "
                    f"got: {pad_name}, options are: {self.source_pad_names}"
                )

    def _coerce_iterator(self, iterable):
        """Coerce the iterable to an iterator if it is not already one.

        Args:
            iterable:
                Iterable, the iterable to coerce

        Returns:
            Iterator, the iterator
        """
        # Check if already an iterator or generator
        if isinstance(iterable, (Iterator, Generator)):
            return iterable

        return iter(iterable)

    def _get_value(self, iterator):
        """Get the next value from the iterator.

        Args:
            iterator:
                Iterator, the iterator to get the value from

        Returns:
            Any, the next value from the iterator
        """
        try:
            return next(iterator)
        except StopIteration:
            return None

    def update(self, pad: SourcePad):
        """Update the iterator for the pad. This is a no-op for IterSource. For
        subclasses that need to update the iterator, this method should be overridden.
        Examples include reading from a file or network stream.

        Args:
            pad:
                SourcePad, the pad to update
        """
        pass

    def new(self, pad: SourcePad) -> Frame:
        """New Frames are created on "pad" with an instance specific count and a name
        derived from the pad name. EOS is set if we have surpassed the requested number
        of Frames.

        Args:
            pad:
                SourcePad, the pad for which to produce a new Frame

        Returns:
            Frame, the Frame with optional data payload
        """
        # Update the pad iterator
        self.update(pad=pad)

        # Get the pad iterator
        assert isinstance(self.iters, dict)
        assert isinstance(self.eos_on_empty, dict)
        pad_iter = self.iters[pad.name]
        pad_eos_on_empty = self.eos_on_empty[pad.name]

        # Get data from the iterator
        data = self._get_value(pad_iter)

        # Return the frame
        return self.frame_factory(EOS=data is None and pad_eos_on_empty, data=data)


@dataclass
class DequeSource(IterSource):
    """A source element that has one double-ended-queue (deque) per source pad.

    The end of stream is controlled by setting an optional limit on the number
    of times a deque can be empty before EOS is signaled.

    Args:
        iters:
            dict[str, deque ], a mapping of source pads to deque s, where the
            key is the pad name and the value is the deque
        eos_on_empty:
            Union[dict[str, bool], bool], default True, a mapping of source
            pads to boolean values, where the key is the pad name and the value
            is the boolean. If a bool is given, the value is applied to all
            pads. If True, EOS is signaled when the deque is empty.
    """

    def _coerce_iterator(self, iterable):
        """Coerce the iterable to an iterator if it is not already one.

        Args:
            iterable:
                Iterable, the iterable to coerce

        Returns:
            Iterator, the iterator
        """
        return deque(iterable)

    def _get_value(self, deque):
        """Get the next value from the deque.

        Args:
            deque :
                deque , the deque to get the value from

        Returns:
            Any, the next value from the deque
        """
        try:
            return deque.pop()
        except IndexError:
            return None

    @property
    def deques(self) -> dict[str, Iterable]:
        """Get the iters property with more explicit alias."""
        assert isinstance(self.iters, dict)
        return self.iters


@dataclass
class StatsSource(SourceElement):
    """A source element that produces system statistics.

    This source collects system statistics using psutil and produces frames
    containing system performance data for the current SGN pipeline and system.

    Args:
        interval:
            Optional[float], time in seconds between stats collection.
            If None, stats are collected every time new() is called.
        include_process_stats:
            bool, whether to include statistics about the current process.
        include_system_stats:
            bool, whether to include system-wide statistics.
        frame_factory:
            Callable, the factory function to create frames.
        eos_on_signal:
            bool, whether to end the stream on receiving a signal (SIGINT/SIGTERM).
        wait:
            Optional[float], time in seconds to wait between frames.
            If None, frames are produced as fast as possible.
    """

    interval: Optional[float] = None
    include_process_stats: bool = True
    include_system_stats: bool = True
    frame_factory: Callable = Frame
    eos_on_signal: bool = True
    wait: Optional[float] = None

    def __post_init__(self):
        """Post initialization setup for StatsSource."""
        super().__post_init__()
        self._last_collection_time = 0.0
        self._eos = False

        # Set up process tracking
        self._current_pid = os.getpid()
        self._current_process = None

        if PSUTIL_AVAILABLE:
            self._current_process = psutil.Process(self._current_pid)
        else:
            warnings.warn(
                "psutil is not installed. StatsSource will provide minimal "
                "functionality. Install with: pip install psutil",
                stacklevel=2,
            )

        # Set up signal handling if requested
        self._signal_handler = None
        if self.eos_on_signal:
            self._signal_handler = SignalEOS

    def _collect_process_stats(self) -> Dict[str, Any]:
        """Collect statistics for the current process.

        Returns:
            Dict containing process statistics.
        """
        if not PSUTIL_AVAILABLE or self._current_process is None:
            return {
                "pid": self._current_pid,
                "error": "psutil not available",
                "limited_info": True,
            }

        proc = self._current_process

        # Basic process info
        proc_info = {
            "pid": proc.pid,
            "name": proc.name(),
            "status": proc.status(),
            "created": proc.create_time(),
            "running_time": time.time() - proc.create_time(),
        }

        # CPU stats
        try:
            proc_info["cpu_percent"] = proc.cpu_percent(interval=None)
            proc_info["cpu_times"] = dict(proc.cpu_times()._asdict())
            proc_info["num_threads"] = proc.num_threads()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        # Memory stats
        try:
            mem_info = proc.memory_info()
            proc_info["memory"] = {
                "rss": mem_info.rss,  # Resident Set Size
                "vms": mem_info.vms,  # Virtual Memory Size
                "shared": getattr(mem_info, "shared", 0),
                "text": getattr(mem_info, "text", 0),
                "data": getattr(mem_info, "data", 0),
            }
            proc_info["memory_percent"] = proc.memory_percent()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        # IO stats
        try:
            io_counters = proc.io_counters()
            proc_info["io"] = {
                "read_count": io_counters.read_count,
                "write_count": io_counters.write_count,
                "read_bytes": io_counters.read_bytes,
                "write_bytes": io_counters.write_bytes,
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
            pass

        return proc_info

    def _collect_system_stats(self) -> Dict[str, Any]:
        """Collect system-wide statistics.

        Returns:
            Dict containing system statistics.
        """
        if not PSUTIL_AVAILABLE:
            return {
                "error": "psutil not available",
                "limited_info": True,
                "system": sys.platform,
                "python_version": sys.version,
            }

        system_stats = {}

        # CPU stats
        system_stats["cpu"] = {
            "percent": psutil.cpu_percent(interval=None),
            "count": {
                "physical": psutil.cpu_count(logical=False),
                "logical": psutil.cpu_count(logical=True),
            },
            "stats": dict(psutil.cpu_stats()._asdict()),
        }

        try:
            system_stats["cpu"]["times"] = dict(psutil.cpu_times()._asdict())
            system_stats["cpu"]["freq"] = (
                dict(psutil.cpu_freq()._asdict()) if psutil.cpu_freq() else {}
            )
        except (AttributeError, OSError):
            pass

        # Memory stats
        system_stats["memory"] = dict(psutil.virtual_memory()._asdict())
        system_stats["swap"] = dict(psutil.swap_memory()._asdict())

        # Disk stats
        try:
            system_stats["disk"] = {
                "usage": dict(psutil.disk_usage("/")._asdict()),
                "io_counters": (
                    dict(psutil.disk_io_counters()._asdict())
                    if psutil.disk_io_counters()
                    else {}
                ),
            }
        except (AttributeError, OSError):
            pass

        # Network stats
        try:
            net_io = psutil.net_io_counters()
            system_stats["network"] = dict(net_io._asdict()) if net_io else {}
        except (AttributeError, OSError):
            pass

        return system_stats

    def should_collect_stats(self) -> bool:
        """Determine if it's time to collect new statistics.

        Returns:
            bool, True if it's time to collect stats based on the interval.
        """
        current_time = time.time()
        if (
            self.interval is None
            or (current_time - self._last_collection_time) >= self.interval
        ):
            self._last_collection_time = current_time
            return True
        return False

    def check_eos(self) -> bool:
        """Check if end-of-stream has been signaled.

        Returns:
            bool, True if EOS should be set.
        """
        if self._eos:
            return True

        if (
            self.eos_on_signal
            and self._signal_handler
            and self._signal_handler.signaled_eos()
        ):
            return True

        return False

    def new(self, pad: SourcePad) -> Frame:
        """Create a new Frame containing system statistics.

        This method is called by the pipeline to produce a new frame with
        current system statistics.

        Args:
            pad: SourcePad, the pad for which to produce a new Frame

        Returns:
            Frame, the Frame containing system statistics
        """
        # Respect the wait parameter if set, adding a delay between frames
        if self.wait is not None:
            time.sleep(self.wait)

        stats: Dict[str, Any] = {}

        # Check if we should collect stats based on the interval
        if self.should_collect_stats():
            # Collect process stats if requested
            if self.include_process_stats:
                stats["process"] = self._collect_process_stats()

            # Collect system stats if requested
            if self.include_system_stats:
                stats["system"] = self._collect_system_stats()

            # Add timestamp
            stats["timestamp"] = float(time.time())

        # Check for EOS condition
        eos = self.check_eos()

        # Create and return the frame
        return self.frame_factory(
            EOS=eos, data=stats, metadata={"stats_type": "system_metrics"}
        )
