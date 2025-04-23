from __future__ import annotations

import multiprocessing
import multiprocessing.shared_memory
from dataclasses import dataclass
from typing import Optional

from sgn import SinkElement, TransformElement
from sgn.base import SGN_LOG_LEVELS, get_sgn_logger
from sgn.sources import SignalEOS

multiprocessing.set_start_method("fork")
LOGGER = get_sgn_logger("subprocess", SGN_LOG_LEVELS)


class SubProcess(SignalEOS):
    """
    A context manager for running SGN pipelines with elements that implement
    separate processes.  This class supports a list of shared memory objects that
    will be managed on exit also: see to_shm().

    pipeline = Pipeline()
    with SubProcess(pipeline) as subprocess:
        subprocess.run()
    """

    shm_list: list = []
    instance_list: list = []

    def __init__(self, pipeline=None):
        self.pipeline = pipeline
        self.multiprocess_enabled = False

    def __enter__(self):
        super().__enter__()
        for e in SubProcess.instance_list:
            e.process.start()
        self.multiprocess_enabled = True
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        super().__exit__(exc_type, exc_value, exc_traceback)
        # rejoin all the processes
        for e in SubProcess.instance_list:
            e.process.join()
        SubProcess.instance_list = []
        # Clean up shared memory
        for d in SubProcess.shm_list:
            multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
        SubProcess.shm_list = []
        self.multiprocess_enabled = False

    @staticmethod
    def to_shm(name, bytez, **kwargs):
        try:
            shm = multiprocessing.shared_memory.SharedMemory(
                name=name, create=True, size=len(bytez)
            )
        except FileExistsError as e:
            print("Shared memory: %s already exists" % name)
            print(
                "You can clear the memory by doing "
                f"multiprocessing.shared_memory.SharedMemory(name='{name}').unlink()\n"
            )
            for d in SubProcess.shm_list:
                multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
            SubProcess.shm_list = []
            raise (e)
        shm.buf[: len(bytez)] = bytez
        out = {"name": name, "shm": shm, **kwargs}
        SubProcess.shm_list.append(out)
        return out

    def run(self):
        assert self.pipeline is not None
        try:
            self.pipeline.run()
        except Exception as e:
            for p in SubProcess.instance_list:
                p.process_stop.set()
            for p in SubProcess.instance_list:
                p.process.join()
            raise RuntimeError(e)
        for p in SubProcess.instance_list:
            p.process_stop.set()


@dataclass
class SubProcessTransformElement(TransformElement, SubProcess):
    """
    A Transform element that runs the function sub_process_internal(shm_list,
    inq, outq, process_stop, process_argdict) in  a separate process. By design
    sub_process_internal(...) does not have a reference to the class or instance
    making it more likely to pickle.  This base class provides all of the arguments
    with user specific arguments being handled by process_argdict.
    """

    process_argdict: Optional[dict] = None

    def __post_init__(self):
        TransformElement.__post_init__(self)
        self.in_queue = multiprocessing.Queue(maxsize=1)
        self.out_queue = multiprocessing.Queue(maxsize=1)
        self.process_stop = multiprocessing.Event()
        self.process = multiprocessing.Process(
            target=self.sub_process_internal,
            args=(
                SubProcess.shm_list,
                self.in_queue,
                self.out_queue,
                self.process_stop,
                self.process_argdict,
            ),
        )
        SubProcess.instance_list.append(self)

    @staticmethod
    def sub_process_internal(shm_list, inq, outq, process_stop, process_argdict):
        raise NotImplementedError


@dataclass
class SubProcessSinkElement(SinkElement, SubProcess):
    """
    A Sink element that runs the function sub_process_internal(shm_list,
    inq, outq, process_stop, process_argdict) in  a separate process. By design
    sub_process_internal(...) does not have a reference to the class or instance
    making it more likely to pickle.  This base class provides all of the arguments
    with user specific arguments being handled by process_argdict.
    """

    process_argdict: Optional[dict] = None
    queue_maxsize: Optional[int] = 100

    def __post_init__(self):
        SinkElement.__post_init__(self)
        self.in_queue = multiprocessing.Queue(maxsize=self.queue_maxsize)
        self.out_queue = multiprocessing.Queue(maxsize=self.queue_maxsize)
        self.process_stop = multiprocessing.Event()
        self.process = multiprocessing.Process(
            target=self.sub_process_internal,
            args=(
                SubProcess.shm_list,
                self.in_queue,
                self.out_queue,
                self.process_stop,
                self.process_argdict,
            ),
        )
        SubProcess.instance_list.append(self)

    @staticmethod
    def sub_process_internal(shm_list, inq, outq, process_stop, process_argdict):
        raise NotImplementedError
