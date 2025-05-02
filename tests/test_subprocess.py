#!/usr/bin/env python3

from __future__ import annotations

import time
import multiprocessing

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


# def test_subprocess_stop():
#    inq = multiprocessing.Queue(maxsize=1)
#    outq = multiprocessing.Queue(maxsize=1)
#    inq.put(None)
#    outq.put(None)
#    _SubProcessTransSink.sub_process_stop(inq=inq, outq=outq)


if __name__ == "__main__":
    test_subprocess()
