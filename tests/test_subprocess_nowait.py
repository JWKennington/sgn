#!/usr/bin/env python3

from __future__ import annotations
import pytest
import time
import multiprocessing.shared_memory
from dataclasses import dataclass
from queue import Empty
from sgn.sources import SignalEOS
from sgn.subprocess import SubProcess, SubProcessTransformElement, SubProcessSinkElement
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
        self.in_queue.put(frame)
        self.in_queue.put(frame)
        self.in_queue.put(frame)
        if frame.EOS:
            self.mark_eos(pad)
            self.sub_process_shutdown(1e-6)

    @staticmethod
    def sub_process_internal(
        **kwargs,
    ):
        kwargs["inq"].get(timeout=1)
        time.sleep(3)
        kwargs["outq"].put(None)


#
# A Transform class that runs its guts in a separate process
#
@dataclass
class MyTransformClass(SubProcessTransformElement):
    def __post_init__(self):
        super().__post_init__()
        assert len(self.sink_pad_names) == 1 and len(self.source_pad_names) == 1

    def pull(self, pad, frame):
        self.in_queue.put(frame)

    @staticmethod
    def sub_process_internal(
        **kwargs,
    ):
        try:
            frame = kwargs["inq"].get(timeout=1)
            kwargs["outq"].put(frame)
        except Empty:
            kwargs["outq"].put(Frame(EOS=True))

    def new(self, pad):
        return self.out_queue.get()


#
# This goes into shared memory
#


def test_subprocess():
    # Make sure instance list is clear before starting the test
    SubProcess.instance_list = []

    # Create a new shared memory segment for this test
    try:
        shared_data = bytearray("Here is a string for nowait test", "utf-8")
        SubProcess.to_shm("nowait_test_mem", shared_data)
    except FileExistsError:
        # Cleanup any existing shared memory with this name
        for d in SubProcess.shm_list:
            try:
                multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
            except FileNotFoundError:
                pass
        SubProcess.shm_list = []
        # Try again
        shared_data = bytearray("Here is a string for nowait test", "utf-8")
        SubProcess.to_shm("nowait_test_mem", shared_data)

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
        with pytest.raises(RuntimeError):
            subprocess.run()

    # Clean up
    for d in SubProcess.shm_list:
        try:
            multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
        except FileNotFoundError:
            pass
    SubProcess.shm_list = []
    SubProcess.instance_list = []


if __name__ == "__main__":
    test_subprocess()
