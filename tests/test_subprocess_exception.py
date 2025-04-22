#!/usr/bin/env python3

from __future__ import annotations
import pytest
from dataclasses import dataclass
from queue import Empty
from sgn.sources import SignalEOS
from sgn.subprocess import SubProcess, SubProcessTransformElement
from sgn.base import SourceElement, SinkElement, Frame
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
# A sink class that does raises an exception
#
class MyBrokenSinkClass(SinkElement):
    def pull(self, pad, frame):
        raise ValueError("Not today!")
        if frame.EOS:
            self.mark_eos(pad)


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
    def sub_process_internal(shm_list, inq, outq, process_stop, argdict):
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


def test_shm_exception():

    shared_data = bytearray(
        "Here is a string that will be shared between processes", "utf-8"
    )
    with pytest.raises(FileExistsError):
        # Trying this again will raise an exception that is trapped
        SubProcess.to_shm("shared_data", shared_data)
        SubProcess.to_shm("shared_data", shared_data)


def test_transform_exception():
    with pytest.raises(NotImplementedError):
        SubProcessTransformElement.sub_process_internal(None, None, None, None, None)


def test_subprocess_exception():

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
    sink = MyBrokenSinkClass(sink_pad_names=("samples1", "samples2"))

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


if __name__ == "__main__":
    test_subprocess_exception()
