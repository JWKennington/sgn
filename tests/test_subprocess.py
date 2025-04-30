#!/usr/bin/env python3

from __future__ import annotations
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
# A simple source class that just sends nothing every 1 second until ctrl+C
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
        self.in_queue.put(frame)

    @staticmethod
    def sub_process_internal(
        shm_list, inq, outq, process_stop, main_thread_exception, argdict
    ):
        while not process_stop.is_set():
            try:
                inq.get(timeout=1)
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

    def pull(self, pad, frame):
        self.in_queue.put(frame)

    @staticmethod
    def sub_process_internal(
        shm_list, inq, outq, process_stop, main_thread_exception, argdict
    ):
        # access some shared memory - there is only one
        shm = shm_list[0]["shm"]
        print(shm.buf)
        while not process_stop.is_set():
            try:
                frame = inq.get(timeout=1)
                outq.put(frame)
            except Empty:
                pass
        outq.put(Frame(EOS=True))

    def new(self, pad):
        return self.out_queue.get()


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


if __name__ == "__main__":
    test_subprocess()
