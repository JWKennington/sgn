from __future__ import annotations

import multiprocessing
import multiprocessing.shared_memory
import time
from dataclasses import dataclass
from typing import Optional

from sgn import SinkElement, TransformElement
from sgn.base import SGN_LOG_LEVELS, get_sgn_logger
from sgn.sources import SignalEOS

try:
    multiprocessing.set_start_method("spawn")
except RuntimeError:
    pass
LOGGER = get_sgn_logger("subprocess", SGN_LOG_LEVELS)


class SubProcess(SignalEOS):
    """
    A context manager for running SGN pipelines with elements that implement
    separate processes.

    This class manages the lifecycle of subprocesses in an SGN pipeline,
    handling process creation, execution, and cleanup. It also supports a list of
    shared memory objects that will be automatically cleaned up on exit through the
    to_shm() method.

    Key features include:
    - Automatic management of process lifecycle (creation, starting, joining, cleanup)
    - Shared memory management for efficient data sharing between processes
    - Signal handling coordination between main process and subprocesses
    - Resilience against KeyboardInterrupt (Ctrl+C) - subprocesses catch and
      ignore these signals, allowing the main process to coordinate a clean shutdown
    - Orderly shutdown to ensure all resources are properly released

    IMPORTANT: All code using the SubProcess context manager MUST be wrapped within
    an if __name__ == "__main__": block. This is required because SGN uses Python's
    multiprocessing module with the 'spawn' start method, which requires that the main
    module be importable.

    Example:
        def main():
            pipeline = Pipeline()
            with SubProcess(pipeline) as subprocess:
                subprocess.run()

        if __name__ == "__main__":
            main()
    """

    shm_list: list = []
    instance_list: list = []
    multiprocess_enabled: bool = False
    # The hard timeout before a subprocess gets a sigkill.  Processes should
    # cleanup after themselves within this time and exit cleanly or else.
    process_join_timeout: float = 5.0

    def __init__(self, pipeline=None):
        self.pipeline = pipeline

    def __enter__(self):
        super().__enter__()
        for e in SubProcess.instance_list:
            e.process.start()
        SubProcess.multiprocess_enabled = True
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        super().__exit__(exc_type, exc_value, exc_traceback)
        # rejoin all the processes
        for e in SubProcess.instance_list:
            if e.in_queue is not None:
                e.in_queue.cancel_join_thread()
            if e.out_queue is not None:
                e.out_queue.cancel_join_thread()
            e.process.join(SubProcess.process_join_timeout)
            e.process.kill()
        SubProcess.instance_list = []
        # Clean up shared memory
        for d in SubProcess.shm_list:
            multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
        SubProcess.shm_list = []
        SubProcess.multiprocess_enabled = False

    @staticmethod
    def to_shm(name, bytez, **kwargs):
        """
        Create a shared memory object that can be accessed by subprocesses.

        This method creates a shared memory segment that will be automatically
        cleaned up when the SubProcess context manager exits. The shared memory can be
        used to efficiently share large data between processes without serialization
        overhead.

        Args:
            name (str): Unique identifier for the shared memory block
            bytez (bytes or bytearray): Data to store in shared memory
            **kwargs: Additional metadata to store with the shared memory reference

        Returns:
            dict: A dictionary containing the shared memory object and metadata
                  with keys:
                - "name": The name of the shared memory block
                - "shm": The SharedMemory object
                - Any additional key-value pairs from kwargs

        Raises:
            FileExistsError: If shared memory with the given name already exists

        Example:
            shared_data = bytearray("Hello world", "utf-8")
            shm_ref = SubProcess.to_shm("example_data", shared_data)
        """
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
        """
        Run the pipeline managed by this SubProcess instance.

        This method executes the associated pipeline and ensures proper cleanup
        of subprocess resources, even in the case of exceptions. It signals all
        subprocesses to stop when the pipeline execution completes or if an exception
        occurs.

        Raises:
            RuntimeError: If an exception occurs during pipeline execution
            AssertionError: If no pipeline was provided to the SubProcess
        """
        assert self.pipeline is not None
        try:
            self.pipeline.run()
        except Exception as e:
            # Signal all processes to stop when an exception occurs
            for p in SubProcess.instance_list:
                p.process_stop.set()
            # Clean up all processes
            for p in SubProcess.instance_list:
                if p.in_queue is not None:
                    p.in_queue.cancel_join_thread()
                if p.out_queue is not None:
                    p.out_queue.cancel_join_thread()
                p.process.join(SubProcess.process_join_timeout)
                p.process.kill()
            raise RuntimeError(e)
        # Signal all processes to stop when pipeline completes normally
        for p in SubProcess.instance_list:
            p.process_stop.set()


@dataclass
class _SubProcessTransSink(SubProcess):
    """
    A mixin class for sharing code between SubProcessTransformElement and
    SubProcessSinkElement.

    This class provides common functionality for both transform and sink
    elements that run in separate processes. It handles the creation and management
    of communication queues, process lifecycle events, and provides methods for
    subprocess synchronization and cleanup.

    Key features:
    - Creates and manages subprocess communication channels (queues)
    - Handles graceful process termination and resource cleanup
    - Provides resilience against KeyboardInterrupt - subprocesses will catch and ignore
      KeyboardInterrupt signals, allowing the main process to handle them and coordinate
      a clean shutdown of all processes
    - Supports orderly shutdown to process remaining queue items before termination

    This is an internal implementation class and should not be instantiated
    directly.  Instead, use SubProcessTransformElement or SubProcessSinkElement.
    """

    process_argdict: Optional[dict] = None
    queue_maxsize: Optional[int] = 100
    err_maxsize: int = 16384

    def __post_init__(self):
        self.in_queue = multiprocessing.Queue(maxsize=self.queue_maxsize)
        self.out_queue = multiprocessing.Queue(maxsize=self.queue_maxsize)
        self.process_stop = multiprocessing.Event()
        self.process_shutdown = multiprocessing.Event()
        self.terminated = multiprocessing.Event()
        self.process = multiprocessing.Process(
            target=self._sub_process_wrapper,
            args=(self.sub_process_internal, self.terminated),
            kwargs={
                "shm_list": SubProcess.shm_list,
                "inq": self.in_queue,
                "outq": self.out_queue,
                "process_stop": self.process_stop,
                "process_shutdown": self.process_shutdown,
                "process_argdict": self.process_argdict,
            },
        )
        SubProcess.instance_list.append(self)

    @staticmethod
    def _sub_process_wrapper(
        func,
        terminated,
        **kwargs,
    ):
        """Internal wrapper method that runs the actual subprocess function.

        This method manages the execution of the subprocess function and handles various
        events and exceptions. It's responsible for:
        1. Running the subprocess function in a loop until stopped
        2. Catching and ignoring KeyboardInterrupt exceptions to prevent subprocesses
           from terminating prematurely when Ctrl+C is pressed
        3. Managing orderly shutdown to drain remaining queue items
        4. Setting the terminated event when the subprocess completes

        Args:
            func: The function to run in the subprocess (typically sub_process_internal)
            terminated: Event that signals when the subprocess has terminated
            **kwargs: Additional keyword arguments to pass to the function
        """
        process_shutdown = kwargs["process_shutdown"]
        process_stop = kwargs["process_stop"]
        inq = kwargs["inq"]

        try:
            while not process_shutdown.is_set() and not process_stop.is_set():
                try:
                    func(**kwargs)
                except KeyboardInterrupt as ei:
                    print("subprocess received, ", repr(ei), " ...continuing.")
                    # Specifically catch and ignore KeyboardInterrupt to prevent
                    # subprocesses from terminating when Ctrl+C is pressed
                    # This allows the main process to handle the interrupt and
                    # coordinate a clean shutdown of all subprocesses
                    continue
            if process_shutdown.is_set() and not process_stop.is_set():
                tries = 0
                num_empty = 3
                while True:
                    if not inq.empty():
                        func(**kwargs)
                        tries = 0  # reset
                    else:
                        time.sleep(1)
                        tries += 1
                        if tries > num_empty:
                            # Try several times to make sure queue is actually empty
                            # FIXME: find a better way
                            break

        except Exception as e:
            print("Exception: ", repr(e))
        terminated.set()
        if process_shutdown.is_set() and not process_stop.is_set():
            while not process_stop.is_set():
                time.sleep(1)
        _SubProcessTransSink._drainqs(**kwargs)

    @staticmethod
    def sub_process_internal(
        **kwargs,
    ):
        """
        Method to be implemented by subclasses. Runs in a separate process.

        This is the main method that will execute in the subprocess. Subclasses must
        override this method to implement their specific processing logic. The method
        receives all necessary resources via kwargs, making it more likely to pickle
        correctly.

        Args:
            shm_list (list): List of shared memory objects created with
                SubProcess.to_shm()
            inq (multiprocessing.Queue): Input queue for receiving data from
                the main process
            outq (multiprocessing.Queue): Output queue for sending data back to
                the main process
            process_stop (multiprocessing.Event): Event that signals when the
                process should stop
            process_shutdown (multiprocessing.Event): Event that signals
                orderly shutdown (process all pending data)
            terminated (multiprocessing.Event): Event that the subprocess sets
                when it has completed processing
            process_argdict (dict, optional): Dictionary of additional
                user-specific arguments

        Note:
            This implementation intentionally does not reference the class or instance,
            which could cause pickling issues when creating the subprocess.

        Raises:
            NotImplementedError: This method must be overridden by subclasses
        """
        raise NotImplementedError

    def sub_process_shutdown(self, timeout=0):
        """
        Initiate an orderly shutdown of the subprocess.

        This method signals the subprocess to complete processing of any pending data
        and then terminate. It waits for the subprocess to indicate completion, and
        collects any remaining data from the output queue before cleaning up resources.

        Args:
            timeout (int, optional): Maximum time in seconds to wait for the subprocess
                to terminate. Defaults to 0 (wait indefinitely).

        Returns:
            list: Any remaining items from the output queue

        Raises:
            RuntimeError: If the subprocess does not terminate within the
            specified timeout
        """
        # Signal subprocess to finish processing pending data
        self.process_shutdown.set()
        start = time.time()
        out = []

        # Wait for subprocess to indicate termination
        while True:
            time.sleep(1)
            if self.terminated.is_set():
                break
            if timeout > 0 and time.time() - start > timeout:
                raise RuntimeError("timeout exceeded for subprocess shutdown")

        # Collect any remaining output data
        if self.out_queue is not None:
            while not self.out_queue.empty():
                out.append(self.out_queue.get_nowait())

        # Signal complete stop and clean up resources
        self.process_stop.set()
        self.in_queue = None
        self.out_queue = None
        return out

    @staticmethod
    def _drainqs(**kwargs):
        """
        Drain and close the input and output queues.

        This is an internal helper method to clean up queues during subprocess
        termination. It removes all items from both input and output queues to
        prevent resource leaks, then closes the queues.

        Args:
            **kwargs: Keyword arguments containing 'inq' and 'outq' keys referencing
                      the input and output multiprocessing.Queue objects

        Note:
            Subclasses can override this method if they need to process remaining
            data in the queues instead of discarding it.
        """
        inq, outq = kwargs["inq"], kwargs["outq"]
        if outq is not None:
            while not outq.empty():
                outq.get(timeout=0.1)
            outq.close()
        if inq is not None:
            while not inq.empty():
                inq.get(timeout=0.1)
            inq.close()

    def internal(self):
        """
        Check for premature process termination.

        This method verifies that the subprocess has not terminated before
        reaching End-Of-Stream (EOS). It is used internally to detect abnormal process
        termination.

        Raises:
            RuntimeError: If the subprocess has terminated but has not reached EOS
        """
        if self.terminated.is_set() and not self.at_eos:
            raise RuntimeError("process stopped before EOS")


@dataclass
class SubProcessTransformElement(TransformElement, _SubProcessTransSink, SubProcess):
    """
    A Transform element that runs processing logic in a separate process.

    This class extends the standard TransformElement to execute its processing in a
    subprocess. It communicates with the main process through input and output queues,
    and manages the lifecycle of the subprocess. Subclasses must implement the
    sub_process_internal method to define the processing logic that runs in the
    subprocess.

    The design intentionally avoids passing class or instance references to the
    subprocess to prevent pickling issues. Instead, it passes all necessary data
    and resources via function arguments.

    The subprocess implementation includes special handling for
    KeyboardInterrupt signals.  When Ctrl+C is pressed in the terminal,
    subprocesses will catch and ignore the KeyboardInterrupt, allowing them to
    continue processing while the main process coordinates a graceful shutdown.
    This prevents data loss and ensures all resources are properly cleaned up.

    Attributes:
        process_argdict (dict, optional): Custom arguments to pass to the subprocess
        queue_maxsize (int, optional): Maximum size of the communication queues
        err_maxsize (int): Maximum size for error data
        at_eos (bool): Flag indicating if End-Of-Stream has been reached

    Example:
        @dataclass
        class MyProcessingElement(SubProcessTransformElement):
            def __post_init__(self):
                super().__post_init__()

            def pull(self, pad, frame):
                # Send the frame to the subprocess
                self.in_queue.put(frame)

            @staticmethod
            def sub_process_internal(**kwargs):
                # Process data in the subprocess
                inq, outq = kwargs["inq"], kwargs["outq"]
                frame = inq.get(timeout=1)
                # Process frame data
                outq.put(processed_frame)

            def new(self, pad):
                # Get processed data from the subprocess
                return self.out_queue.get()
    """

    at_eos: bool = False

    internal = _SubProcessTransSink.internal

    def __post_init__(self):
        TransformElement.__post_init__(self)
        _SubProcessTransSink.__post_init__(self)


@dataclass
class SubProcessSinkElement(SinkElement, _SubProcessTransSink, SubProcess):
    """
    A Sink element that runs data consumption logic in a separate process.

    This class extends the standard SinkElement to execute its processing in a
    subprocess. It communicates with the main process through input and output queues,
    and manages the lifecycle of the subprocess. Subclasses must implement the
    sub_process_internal method to define the consumption logic that runs in the
    subprocess.

    The design intentionally avoids passing class or instance references to the
    subprocess to prevent pickling issues. Instead, it passes all necessary data
    and resources via function arguments.

    The subprocess implementation includes special handling for
    KeyboardInterrupt signals.  When Ctrl+C is pressed in the terminal,
    subprocesses will catch and ignore the KeyboardInterrupt, allowing them to
    continue processing while the main process coordinates a graceful shutdown.
    This prevents data loss and ensures all resources are properly cleaned up.

    Attributes:
        process_argdict (dict, optional): Custom arguments to pass to the subprocess
        queue_maxsize (int, optional): Maximum size of the communication queues
        err_maxsize (int): Maximum size for error data

    Example:
        @dataclass
        class MyLoggingSinkElement(SubProcessSinkElement):
            def __post_init__(self):
                super().__post_init__()

            def pull(self, pad, frame):
                if frame.EOS:
                    self.mark_eos(pad)
                # Send the frame to the subprocess
                self.in_queue.put((pad.name, frame))

            @staticmethod
            def sub_process_internal(**kwargs):
                inq, process_stop = kwargs["inq"], kwargs["process_stop"]

                try:
                    # Get data from the main process
                    pad_name, frame = inq.get(timeout=1)

                    # Process or log the data
                    if not frame.EOS:
                        print(f"Sink received on {pad_name}: {frame.data}")
                    else:
                        print(f"Sink received EOS on {pad_name}")

                except Empty:
                    pass
    """

    internal = _SubProcessTransSink.internal

    def __post_init__(self):
        SinkElement.__post_init__(self)
        _SubProcessTransSink.__post_init__(self)
