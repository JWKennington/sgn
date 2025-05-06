#!/usr/bin/env python3

from __future__ import annotations

import time
import multiprocessing
import pytest
import threading
from dataclasses import dataclass
from queue import Empty
from sgn.sources import SignalEOS
from sgn.subprocess import (
    SubProcess,
    _SubProcessTransSink,
    SubProcessTransformElement,
    SubProcessSinkElement,
)
from sgn.base import SourceElement, Frame
from sgn.apps import Pipeline
import ctypes


def get_address(buffer):
    address = ctypes.addressof(ctypes.c_char.from_buffer(buffer))
    return address


#
# A simple source class that just sends and EOS frame
#


class MySourceClass(SourceElement, SignalEOS):
    def new(self, pad):
        return Frame(data=None, EOS=True)


#
# A sink class that does nothing
#
@dataclass
class MySinkClass(SubProcessSinkElement):
    def __post_init__(self):
        super().__post_init__()

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        if self.at_eos and not self.terminated.is_set():
            self.in_queue.put(frame)
            self.sub_process_shutdown(10)

    @staticmethod
    def sub_process_internal(
        **kwargs,
    ):
        kwargs["outq"].put(None)
        try:
            kwargs["inq"].get(timeout=1)
        except Empty:
            pass


#
# A Transform class that runs its guts in a separate process
#
@dataclass
class MyTransformClass(SubProcessTransformElement):
    def __post_init__(self):
        super().__post_init__()
        assert len(self.sink_pad_names) == 1 and len(self.source_pad_names) == 1
        self.at_eos = False
        self.frame_list = []

    def pull(self, pad, frame):
        self.in_queue.put(frame)
        if frame.EOS and not self.terminated.is_set():
            self.at_eos = True
            self.frame_list = self.sub_process_shutdown(10)

    @staticmethod
    def sub_process_internal(
        **kwargs,
    ):
        # access some shared memory - there is only one
        shm = kwargs["shm_list"][0]["shm"]
        print(shm.buf)
        try:
            frame = kwargs["inq"].get(timeout=1)
            kwargs["outq"].put(frame)
        except Empty:
            pass

    def new(self, pad):
        return self.frame_list[0]


#
# This goes into shared memory
#


def test_subprocess():

    shared_data = bytearray(
        "Here is a string that will be shared between processes", "utf-8"
    )
    SubProcess.to_shm("shared_data", shared_data)

    source = MySourceClass(source_pad_names=("event",))
    transform1 = MyTransformClass(
        sink_pad_names=("event",), source_pad_names=("samples1",)
    )
    transform2 = MyTransformClass(
        sink_pad_names=("event",), source_pad_names=("samples2",)
    )
    sink = MySinkClass(sink_pad_names=("samples1", "samples2"))

    pipeline = Pipeline()

    pipeline.insert(
        source,
        transform1,
        transform2,
        sink,
        link_map={
            sink.snks["samples1"]: transform1.srcs["samples1"],
            sink.snks["samples2"]: transform2.srcs["samples2"],
            transform1.snks["event"]: source.srcs["event"],
            transform2.snks["event"]: source.srcs["event"],
        },
    )

    with SubProcess(pipeline) as subprocess:
        # This will cause the processes to die **AFTER** the pipeline
        # completes.  Internally this also calls pipeline.run()
        subprocess.run()


def test_subprocess_wrapper():
    terminated = multiprocessing.Event()
    shutdown = multiprocessing.Event()
    stop = multiprocessing.Event()
    shutdown.set()
    stop.set()
    inq = multiprocessing.Queue(maxsize=1)
    outq = multiprocessing.Queue(maxsize=1)

    def func(**kwargs):
        pass

    _SubProcessTransSink._sub_process_wrapper(
        func,
        terminated,
        process_shutdown=shutdown,
        process_stop=stop,
        inq=inq,
        outq=outq,
    )


def test_subprocess_wrapper_2():
    terminated = multiprocessing.Event()
    shutdown = multiprocessing.Event()
    stop = multiprocessing.Event()
    inq = multiprocessing.Queue(maxsize=1)
    outq = multiprocessing.Queue(maxsize=1)

    def func(**kwargs):
        raise RuntimeError("nope")

    _SubProcessTransSink._sub_process_wrapper(
        func,
        terminated,
        process_shutdown=shutdown,
        process_stop=stop,
        inq=inq,
        outq=outq,
    )


def test_subprocess_wrapper_3():
    terminated = threading.Event()
    shutdown = threading.Event()
    stop = threading.Event()
    shutdown.set()
    inq = multiprocessing.Queue(maxsize=1)
    inq.put(None)
    outq = multiprocessing.Queue(maxsize=1)
    outq.put(None)

    def func(**kwargs):
        # time.sleep(1)
        raise ValueError("nope")

    thread = threading.Thread(
        target=_SubProcessTransSink._sub_process_wrapper,
        args=(func, terminated),
        kwargs={
            "process_shutdown": shutdown,
            "process_stop": stop,
            "inq": inq,
            "outq": outq,
        },
    )
    thread.start()
    time.sleep(1)
    stop.set()
    shutdown.set()
    thread.join()


def test_subprocess_drain_queue():
    """
    Test specifically targeting the queue draining logic in _sub_process_wrapper
    during orderly shutdown (lines 207-214).
    """
    # Set up events and queues
    terminated = multiprocessing.Event()
    process_shutdown = multiprocessing.Event()
    process_stop = multiprocessing.Event()

    # Set shutdown but not stop - this is the key condition for drain logic
    process_shutdown.set()

    # Set up queue with items to process
    inq = multiprocessing.Queue(maxsize=5)
    for i in range(3):
        inq.put(Frame(data=f"Test Item {i}", EOS=False))
    outq = multiprocessing.Queue(maxsize=5)

    # Track calls to func
    call_count = 0

    # This function will be called to process each item from the queue
    def test_func(**kwargs):
        nonlocal call_count
        q = kwargs["inq"]
        try:
            item = q.get(block=False)
            call_count += 1
            # Simulate processing by printing
            print(f"Processing item: {item.data}")
        except Empty:
            pass

    # Use a thread so we can set process_stop after a delay
    def run_wrapper():
        _SubProcessTransSink._sub_process_wrapper(
            test_func,
            terminated,
            process_shutdown=process_shutdown,
            process_stop=process_stop,
            inq=inq,
            outq=outq,
        )

    # Start thread
    thread = threading.Thread(target=run_wrapper)
    thread.daemon = True
    thread.start()

    # Let it run for a bit to process the queue
    time.sleep(2)

    # Now set stop to allow the thread to exit
    process_stop.set()
    thread.join(timeout=5)

    # Verify items were processed
    assert (
        call_count >= 3
    ), f"Expected at least 3 calls to process items, got {call_count}"
    assert terminated.is_set(), "The terminated event should be set"


# Test for NotImplementedError in sub_process_internal (line 259)
def test_subprocess_internal_not_implemented():
    """Test that _SubProcessTransSink.sub_process_internal raises
    NotImplementedError."""
    with pytest.raises(NotImplementedError):
        _SubProcessTransSink.sub_process_internal()


# Test for RuntimeError in internal method (line 343)
def test_subprocess_internal_runtime_error():
    """Test that internal method raises RuntimeError when terminated is set but
    at_eos is False."""

    class TestSubProcessElement(_SubProcessTransSink):
        def __init__(self):
            self.terminated = multiprocessing.Event()
            self.terminated.set()  # Set terminated
            self.at_eos = False  # But not at_eos

    element = TestSubProcessElement()
    with pytest.raises(RuntimeError):
        element.internal()


# Test for timeout in sub_process_shutdown (line 291)
def test_subprocess_shutdown_timeout():
    """Test that sub_process_shutdown raises RuntimeError on timeout."""

    class TestSubProcessElement(_SubProcessTransSink):
        def __init__(self):
            self.process_shutdown = multiprocessing.Event()
            self.process_stop = multiprocessing.Event()
            self.terminated = multiprocessing.Event()
            self.out_queue = multiprocessing.Queue()
            self.in_queue = multiprocessing.Queue()

    element = TestSubProcessElement()
    # Set a very small timeout to trigger the timeout error
    with pytest.raises(RuntimeError):
        element.sub_process_shutdown(timeout=0.01)


# Test for exception handling in run method (lines 131-143)
def test_subprocess_run_exception():
    """Test that the run method properly handles exceptions in the pipeline."""

    class TestPipeline:
        def run(self):
            raise ValueError("Test exception")

    # Create a mock subprocesses in the instance_list to test the cleanup code
    SubProcess.instance_list = []

    # Create test process with in_queue and out_queue
    process = multiprocessing.Process(target=lambda: None)
    process.start()

    class MockInstance:
        def __init__(self):
            self.process = process
            self.in_queue = multiprocessing.Queue(maxsize=1)
            self.out_queue = multiprocessing.Queue(maxsize=1)
            self.process_stop = multiprocessing.Event()

    # Add to the instance list so it gets cleaned up
    mock_instance = MockInstance()
    SubProcess.instance_list.append(mock_instance)

    # Now run the test
    subprocess = SubProcess(TestPipeline())
    with pytest.raises(RuntimeError):
        subprocess.run()

    # Verify cleanup
    assert mock_instance.process_stop.is_set(), "Process stop event should be set"
    # The instance_list is only cleared in __exit__, not in the exception
    # handler of run()


# Test for FileExistsError handling in to_shm (lines 100-109)
def test_subprocess_to_shm_duplicate():
    """Test that attempting to create duplicate shared memory raises FileExistsError."""
    # First clear any existing shared memory with this name
    try:
        SubProcess.shm_list = []
        multiprocessing.shared_memory.SharedMemory(name="test_duplicate").unlink()
    except FileNotFoundError:
        # This is fine - means the memory segment doesn't exist yet
        pass

    # Create the first shared memory instance
    test_data = bytearray("Test data for shared memory duplicate test", "utf-8")
    SubProcess.to_shm("test_duplicate", test_data)

    # Now try to create another with the same name, which should fail
    with pytest.raises(FileExistsError):
        SubProcess.to_shm("test_duplicate", test_data)

    # Clean up
    for d in SubProcess.shm_list:
        try:
            multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
        except FileNotFoundError:
            pass
    SubProcess.shm_list = []


if __name__ == "__main__":
    test_subprocess()
