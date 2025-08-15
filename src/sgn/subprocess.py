from __future__ import annotations

import inspect
import logging
import multiprocessing
import multiprocessing.shared_memory
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from sgn import SinkElement, TransformElement
from sgn.base import SourceElement
from sgn.frames import Frame
from sgn.sources import SignalEOS

logger = logging.getLogger("sgn.subprocess")


class WorkerContext:
    """Context object passed to worker methods with clean access to resources."""

    def __init__(
        self,
        input_queue=None,
        output_queue=None,
        worker_stop=None,
        worker_shutdown=None,
        **kwargs,
    ):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.stop_event = worker_stop
        self.shutdown_event = worker_shutdown
        self.shared_memory = kwargs.get("shm_list", [])
        self.state = {}  # Persistent dict for worker state across calls
        self._raw_kwargs = kwargs

    def should_stop(self) -> bool:
        """Check if the worker should stop processing."""
        return self.stop_event and self.stop_event.is_set()

    def should_shutdown(self) -> bool:
        """Check if the worker should shutdown gracefully."""
        return self.shutdown_event and self.shutdown_event.is_set()


class Parallelize(SignalEOS):
    """
    A context manager for running SGN pipelines with elements that implement
    separate processes or threads.

    This class manages the lifecycle of workers (processes or threads) in an SGN
    pipeline, handling worker creation, execution, and cleanup. It also supports
    shared memory objects that will be automatically cleaned up on exit through the
    to_shm() method (only applicable for process mode).

    Key features include:
    - Automatic management of worker lifecycle (creation, starting, joining, cleanup)
    - Shared memory management for efficient data sharing (process mode only)
    - Signal handling coordination between main process/thread and workers
    - Resilience against KeyboardInterrupt (Ctrl+C) - workers catch and ignore these
      signals, allowing the main process to coordinate a clean shutdown
    - Orderly shutdown to ensure all resources are properly released
    - Support for both multiprocessing and threading concurrency models
    - Automatic detection and invocation when pipeline.run() is called

    IMPORTANT: When using process mode, code using Parallelize MUST be
    wrapped within an if __name__ == "__main__": block. This is required because SGN
    uses Python's multiprocessing module with the 'spawn' start method, which requires
    that the main module be importable.

    Example with automatic parallelization (RECOMMENDED):
        def main():
            pipeline = Pipeline()
            # Add ParallelizeTransformElement, ParallelizeSinkElement, etc.
            pipeline.run()  # Automatically detects and enables parallelization

        if __name__ == "__main__":
            main()

    Example with manual context manager (LEGACY):
        def main():
            pipeline = Pipeline()
            with Parallelize(pipeline) as parallelize:
                parallelize.run()

        if __name__ == "__main__":
            main()
    """

    shm_list: list = []
    instance_list: list = []
    enabled: bool = False
    # The hard timeout before a worker gets terminated.
    # Workers should cleanup after themselves within this time and exit cleanly.
    # This is a "global" property applied to all subprocesses / subthreads
    join_timeout: float = 10.0  # Increased for CI environments
    # Default flag for whether to use threading (False means use multiprocessing)
    use_threading_default: bool = False
    # Instance variable for thread mode
    use_threading: bool = False

    def __init__(self, pipeline=None, use_threading: Optional[bool] = None):
        """
        Initialize the Parallelize context manager.

        Args:
            pipeline: The pipeline to run
            use_threading: Whether to use threading instead of multiprocessing.
                          If not specified, uses the use_threading_default
        """
        self.pipeline = pipeline
        # Use the specified mode, or fall back to the class default
        self.use_threading = (
            use_threading
            if use_threading is not None
            else Parallelize.use_threading_default
        )

    def __enter__(self):
        try:
            multiprocessing.set_start_method("spawn")
        except RuntimeError:
            pass
        super().__enter__()
        for e in Parallelize.instance_list:
            e.worker.start()
        Parallelize.enabled = True
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        super().__exit__(exc_type, exc_value, exc_traceback)
        # rejoin all the workers
        for e in Parallelize.instance_list:
            if e.in_queue is not None and hasattr(e.in_queue, "cancel_join_thread"):
                e.in_queue.cancel_join_thread()
            if e.out_queue is not None and hasattr(e.out_queue, "cancel_join_thread"):
                e.out_queue.cancel_join_thread()

            if (
                e.worker is not None
                and hasattr(e.worker, "is_alive")
                and e.worker.is_alive()
            ):
                e.worker.join(Parallelize.join_timeout)
                # Only processes can be killed, threads will naturally terminate
                if hasattr(e.worker, "kill") and e.worker.is_alive():
                    e.worker.kill()

        Parallelize.instance_list = []

        # Clean up shared memory (only applicable for process mode)
        if not self.use_threading:
            for d in Parallelize.shm_list:
                multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
            Parallelize.shm_list = []

        Parallelize.enabled = False

    @staticmethod
    def to_shm(name, bytez, **kwargs):
        """
        Create a shared memory object that can be accessed by subprocesses.

        Note: This is only applicable in process mode. In thread mode, shared memory
        is not necessary since threads share the same address space.

        This method creates a shared memory segment that will be automatically
        cleaned up when the Parallelize context manager exits. The shared memory can be
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
            print(f"Shared memory: {name} already exists")
            print(
                "You can clear the memory by doing "
                f"multiprocessing.shared_memory.SharedMemory(name='{name}').unlink()\n"
            )
            for d in Parallelize.shm_list:
                multiprocessing.shared_memory.SharedMemory(name=d["name"]).unlink()
            Parallelize.shm_list = []
            raise e

        shm.buf[: len(bytez)] = bytez
        out = {"name": name, "shm": shm, **kwargs}
        Parallelize.shm_list.append(out)
        return out

    def run(self):
        """
        Run the pipeline managed by this Parallelize instance.

        This method executes the associated pipeline and ensures proper cleanup
        of worker resources, even in the case of exceptions. It signals all
        workers to stop when the pipeline execution completes or if an exception
        occurs.

        Raises:
            RuntimeError: If an exception occurs during pipeline execution
            AssertionError: If no pipeline was provided to the SubProcess
        """
        assert (
            self.pipeline is not None
        ), "Pipeline must be provided to Parallelize constructor before running"
        try:
            # Disable auto-parallelization since we're already in a Parallelize context
            self.pipeline.run(auto_parallelize=False)
        except Exception as e:
            # Signal all workers to stop when an exception occurs
            for p in Parallelize.instance_list:
                p.worker_stop.set()

            # Clean up all workers
            for p in Parallelize.instance_list:
                if p.in_queue is not None and hasattr(p.in_queue, "cancel_join_thread"):
                    p.in_queue.cancel_join_thread()
                if p.out_queue is not None and hasattr(
                    p.out_queue, "cancel_join_thread"
                ):
                    p.out_queue.cancel_join_thread()

                if (
                    p.worker is not None
                    and hasattr(p.worker, "is_alive")
                    and p.worker.is_alive()
                ):
                    p.worker.join(Parallelize.join_timeout)
                    if hasattr(p.worker, "kill") and p.worker.is_alive():
                        p.worker.kill()
            raise RuntimeError(e)

        # Signal all workers to stop when pipeline completes normally
        for p in Parallelize.instance_list:
            p.worker_stop.set()

    @staticmethod
    def needs_parallelization(pipeline):
        """
        Check if a pipeline contains any elements that require parallelization.

        Args:
            pipeline: The Pipeline instance to check

        Returns:
            bool: True if the pipeline contains any Parallelize* elements
        """
        # Check if any element is a subclass of _ParallelizeBase
        return any(
            isinstance(element, _ParallelizeBase) for element in pipeline.elements
        )


@dataclass
class _ParallelizeBase(Parallelize):
    """
    A mixin class for sharing code between ParallelizeTransformElement and
    ParallelizeSinkElement.

    This class provides common functionality for both transform and sink
    elements that run in separate processes or threads. It handles the creation and
    management of communication queues, worker lifecycle events, and provides methods
    for worker synchronization and cleanup.

    Key features:
    - Creates and manages worker communication channels (queues)
    - Handles graceful worker termination and resource cleanup
    - Provides resilience against KeyboardInterrupt - workers will catch and ignore
      KeyboardInterrupt signals, allowing the main process to handle them and coordinate
      a clean shutdown of all workers
    - Supports orderly shutdown to process remaining queue items before termination

    This is an internal implementation class and should not be instantiated
    directly. Instead, use ParallelizeTransformElement or ParallelizeSinkElement.

    Developer Usage:
        @dataclass
        class MyElement(ParallelizeTransformElement):
            multiplier: int = 2
            threshold: float = 0.5

            @staticmethod
            def worker_process(
                context: WorkerContext, multiplier: int, threshold: float
            ):
                try:
                    frame = context.input_queue.get(timeout=1.0)
                    if frame and frame.data > threshold:
                        frame.data *= multiplier
                        context.output_queue.put(frame)
                except queue.Empty:
                    pass
    """

    queue_maxsize: Optional[int] = 100
    err_maxsize: int = 16384
    # Flag that can be set by subclasses to override the default
    _use_threading_override: Optional[bool] = None

    def __post_init__(self):
        # Determine whether to use threading
        self.use_threading = (
            self._use_threading_override
            if self._use_threading_override is not None
            else Parallelize.use_threading_default
        )

        # Extract worker parameters automatically from instance attributes
        worker_params = self._extract_worker_parameters()

        # Create appropriate queue based on mode
        if self.use_threading:
            self.in_queue = queue.Queue(maxsize=self.queue_maxsize)
            self.out_queue = queue.Queue(maxsize=self.queue_maxsize)
            self.worker_stop = threading.Event()
            self.worker_shutdown = threading.Event()
            self.terminated = threading.Event()
            self.worker = threading.Thread(
                target=self._worker_wrapper,
                args=(self.terminated,),
                kwargs={
                    "shm_list": Parallelize.shm_list,
                    "inq": self.in_queue,
                    "outq": self.out_queue,
                    "worker_stop": self.worker_stop,
                    "worker_shutdown": self.worker_shutdown,
                    **worker_params,
                },
                daemon=False,  # Ensure the thread doesn't terminate too early
            )
        else:
            self.in_queue = multiprocessing.Queue(maxsize=self.queue_maxsize)
            self.out_queue = multiprocessing.Queue(maxsize=self.queue_maxsize)
            self.worker_stop = multiprocessing.Event()
            self.worker_shutdown = multiprocessing.Event()
            self.terminated = multiprocessing.Event()
            self.worker = multiprocessing.Process(
                target=self._worker_wrapper,
                args=(self.terminated,),
                kwargs={
                    "shm_list": Parallelize.shm_list,
                    "inq": self.in_queue,
                    "outq": self.out_queue,
                    "worker_stop": self.worker_stop,
                    "worker_shutdown": self.worker_shutdown,
                    **worker_params,
                },
                daemon=False,  # Ensure the process doesn't terminate too early
            )

        # Add to the global instance list
        Parallelize.instance_list.append(self)

    def _extract_worker_parameters(self):
        """Extract parameters for worker_process method from instance attributes."""
        # Get the signature of the worker_process method
        sig = inspect.signature(self.worker_process)
        extracted = {}

        for param_name, param in sig.parameters.items():
            if param_name in ("self", "context"):  # Skip special parameters
                continue

            # Try to get the value from instance attributes
            if hasattr(self, param_name):
                extracted[param_name] = getattr(self, param_name)
            elif param.default is not param.empty:
                # Use default value if available
                extracted[param_name] = param.default

        return extracted

    def _worker_wrapper(self, terminated, **kwargs):
        """Clean wrapper that calls the user's worker_process method."""
        # Create the context object with clean queue wrappers
        context = WorkerContext(
            input_queue=kwargs.get("inq"),
            output_queue=kwargs.get("outq"),
            worker_stop=kwargs.get("worker_stop"),
            worker_shutdown=kwargs.get("worker_shutdown"),
            shm_list=kwargs.get("shm_list", []),
        )

        # Extract worker parameters (excluding the special ones)
        worker_params = {}
        special_keys = {"shm_list", "inq", "outq", "worker_stop", "worker_shutdown"}
        for key, value in kwargs.items():
            if key not in special_keys:
                worker_params[key] = value

        try:
            while not context.should_stop() and not context.should_shutdown():
                try:
                    # Call the user's clean worker_process method
                    self.worker_process(context, **worker_params)
                except KeyboardInterrupt:
                    # Specifically catch and ignore KeyboardInterrupt to prevent
                    # workers from terminating when Ctrl+C is pressed
                    # This allows the main process to handle the interrupt and
                    # coordinate a clean shutdown of all workers
                    print("worker received KeyboardInterrupt...continuing.")
                    continue

            # Handle graceful shutdown
            if context.should_shutdown() and not context.should_stop():
                tries = 0
                num_empty = 3
                while True:
                    try:
                        # Check if input queue is empty
                        is_empty = (
                            context.input_queue.empty() if context.input_queue else True
                        )

                        if not is_empty:
                            self.worker_process(context, **worker_params)
                            tries = 0  # reset
                        else:
                            time.sleep(1)
                            tries += 1
                            if tries > num_empty:
                                # Try several times to make sure queue is actually empty
                                # FIXME: find a better way
                                break
                    except (queue.Empty, Exception):
                        time.sleep(1)
                        tries += 1
                        if tries > num_empty:
                            break

        except Exception as e:
            print("Exception in worker:", repr(e))
        finally:
            terminated.set()
            if context.should_shutdown() and not context.should_stop():
                while not context.should_stop():
                    time.sleep(1)
            self._drain_queues(
                input_queue=kwargs.get("inq"), output_queue=kwargs.get("outq")
            )

    def worker_process(self, context: WorkerContext, *args: Any, **kwargs: Any) -> None:
        """Override this method in subclasses to implement worker logic.

        This method should be implemented as a static method or avoid accessing
        instance attributes directly to prevent pickling issues in multiprocessing mode.
        All necessary data should be passed through the method parameters.

        Args:
            context: WorkerContext with clean access to queues and events
            *args: Automatically extracted instance attributes
            **kwargs: Automatically extracted instance attributes with defaults
        """
        raise NotImplementedError("Subclasses must implement worker_process method")

    @staticmethod
    def _drain_queues(input_queue=None, output_queue=None):
        """Drain and close the input and output queues.

        Args:
            input_queue: The input queue to drain and close
            output_queue: The output queue to drain and close
        """
        # Drain output queue
        if output_queue is not None:
            try:
                while True:
                    if hasattr(output_queue, "get_nowait"):
                        output_queue.get_nowait()
            except (queue.Empty, Exception):
                pass
            # Close the queue if it supports closing
            if hasattr(output_queue, "close"):
                output_queue.close()

        # Drain input queue
        if input_queue is not None:
            try:
                while True:
                    if hasattr(input_queue, "get_nowait"):
                        input_queue.get_nowait()
            except (queue.Empty, Exception):
                pass
            # Close the queue if it supports closing
            if hasattr(input_queue, "close"):
                input_queue.close()

    def sub_process_shutdown(self, timeout=0):
        """
        Initiate an orderly shutdown of the worker.

        This method signals the worker to complete processing of any pending data
        and then terminate. It waits for the worker to indicate completion, and
        collects any remaining data from the output queue before cleaning up resources.

        Args:
            timeout (int, optional): Maximum time in seconds to wait for the worker
                to terminate. Defaults to 0 (wait indefinitely).

        Returns:
            list: Any remaining items from the output queue

        Raises:
            RuntimeError: If the worker does not terminate within the
            specified timeout
        """
        # Signal worker to finish processing pending data
        self.worker_shutdown.set()
        start = time.time()
        out = []

        # Wait for worker to indicate termination
        while True:
            time.sleep(1)
            if self.terminated.is_set():
                break
            if timeout > 0 and time.time() - start > timeout:
                raise RuntimeError("timeout exceeded for worker shutdown")

        # Collect any remaining output data
        if self.out_queue is not None:
            try:
                while True:
                    # Queue.empty() is not reliable, so we use get_nowait()
                    if hasattr(self.out_queue, "get_nowait"):
                        out.append(self.out_queue.get_nowait())
            except (queue.Empty, Exception):
                pass  # Queue is empty

        # Signal complete stop and clean up resources
        self.worker_stop.set()
        self.in_queue = None
        self.out_queue = None
        return out

    def internal(self):
        """
        Check for premature worker termination.

        This method verifies that the worker has not terminated before
        reaching End-Of-Stream (EOS). It is used internally to detect abnormal worker
        termination.

        Raises:
            RuntimeError: If the worker has terminated but has not reached EOS
        """
        if self.terminated.is_set() and not self.at_eos:
            raise RuntimeError("worker stopped before EOS")


@dataclass
class ParallelizeTransformElement(TransformElement, _ParallelizeBase, Parallelize):
    """
    A Transform element that runs processing logic in a separate process or thread.

    This class extends the standard TransformElement to execute its processing in a
    separate worker (process or thread). It communicates with the main process/thread
    through input and output queues, and manages the worker lifecycle. Subclasses must
    implement the worker_process method to define the processing logic that runs
    in the worker.

    The design intentionally avoids passing class or instance references to the
    worker to prevent pickling issues when using process mode. Instead, it passes all
    necessary data and resources via function arguments.

    The implementation includes special handling for KeyboardInterrupt signals.
    When Ctrl+C is pressed in the terminal, workers will catch and ignore the
    KeyboardInterrupt, allowing them to continue processing while the main process
    coordinates a graceful shutdown. This prevents data loss and ensures all resources
    are properly cleaned up.

    Attributes:
        queue_maxsize (int, optional): Maximum size of the communication queues
        err_maxsize (int): Maximum size for error data
        at_eos (bool): Flag indicating if End-Of-Stream has been reached
        _use_threading_override (bool, optional): Set to True to use threading or
            False to use multiprocessing. If not specified, uses the
            Parallelize.use_threading_default

    Example with default process mode:
        @dataclass
        class MyProcessingElement(ParallelizeTransformElement):
            multiplier: int = 2  # Instance attributes become worker parameters

            def pull(self, pad, frame):
                # Send the frame to the worker
                self.in_queue.put(frame)
                if frame.EOS:
                    self.at_eos = True

            def worker_process(self, context: WorkerContext, multiplier: int):
                # Process data in the worker using the clean context
                try:
                    frame = context.input_queue.get(timeout=0.1)
                    if frame and not frame.EOS:
                        frame.data *= multiplier
                        context.output_queue.put(frame)
                except queue.Empty:
                    pass

            def new(self, pad):
                # Get processed data from the worker
                return self.out_queue.get()

    Example with thread mode:
        @dataclass
        class MyThreadedElement(ParallelizeTransformElement):
            _use_threading_override = True
            # Implementation same as above

    Example:
        @dataclass
        class MyProcessingElement(ParallelizeTransformElement):
            multiplier: int = 2
            threshold: float = 0.5

            def pull(self, pad, frame):
                self.in_queue.put(frame)
                if frame.EOS:
                    self.at_eos = True

            def worker_process(
                self, context: WorkerContext, multiplier: int, threshold: float
            ):
                try:
                    frame = context.input_queue.get(timeout=0.1)
                    if frame and not frame.EOS and frame.data > threshold:
                        frame.data *= multiplier
                        context.output_queue.put(frame)
                except queue.Empty:
                    pass

            def new(self, pad):
                return self.out_queue.get()
    """

    at_eos: bool = False

    internal = _ParallelizeBase.internal

    def __post_init__(self):
        TransformElement.__post_init__(self)
        _ParallelizeBase.__post_init__(self)


@dataclass
class ParallelizeSinkElement(SinkElement, _ParallelizeBase, Parallelize):
    """
    A Sink element that runs data consumption logic in a separate process or thread.

    This class extends the standard SinkElement to execute its processing in a
    separate worker (process or thread). It communicates with the main process/thread
    through input and output queues, and manages the worker lifecycle. Subclasses must
    implement the worker_process method to define the consumption logic that runs
    in the worker.

    The design intentionally avoids passing class or instance references to the
    worker to prevent pickling issues when using process mode. Instead, it passes all
    necessary data and resources via function arguments.

    The implementation includes special handling for KeyboardInterrupt signals.
    When Ctrl+C is pressed in the terminal, workers will catch and ignore the
    KeyboardInterrupt, allowing them to continue processing while the main process
    coordinates a graceful shutdown. This prevents data loss and ensures all resources
    are properly cleaned up.

    Attributes:
        queue_maxsize (int, optional): Maximum size of the communication queues
        err_maxsize (int): Maximum size for error data
        _use_threading_override (bool, optional): Set to True to use threading or
            False to use multiprocessing. If not specified, uses the
            Parallelize.use_threading_default

    Example with default process mode:
        @dataclass
        class MyLoggingSinkElement(ParallelizeSinkElement):
            def pull(self, pad, frame):
                if frame.EOS:
                    self.mark_eos(pad)
                # Send the frame to the worker
                self.in_queue.put((pad.name, frame))

            def worker_process(self, context: WorkerContext):
                try:
                    # Get data from the main process/thread
                    pad_name, frame = context.input_queue.get(timeout=0.1)

                    # Process or log the data
                    if not frame.EOS:
                        print(f"Sink received on {pad_name}: {frame.data}")
                    else:
                        print(f"Sink received EOS on {pad_name}")

                except queue.Empty:
                    pass

    Example with thread mode:
        @dataclass
        class MyThreadedSinkElement(ParallelizeSinkElement):
            _use_threading_override = True
            # Implementation same as above
    """

    internal = _ParallelizeBase.internal

    def __post_init__(self):
        SinkElement.__post_init__(self)
        _ParallelizeBase.__post_init__(self)


@dataclass
class ParallelizeSourceElement(SourceElement, _ParallelizeBase, Parallelize):
    """
    A Source element that generates data in a separate process or thread.

    This class extends the standard SourceElement to execute its data generation logic
    in a separate worker (process or thread). It communicates with the main process
    through output queues, and manages the worker lifecycle. Subclasses must implement
    the worker_process method to define the data generation logic that runs in
    the worker.

    The design intentionally avoids passing class or instance references to the
    worker to prevent pickling issues when using process mode. Instead, it passes all
    necessary data and resources via function arguments.

    The implementation includes special handling for KeyboardInterrupt signals.
    When Ctrl+C is pressed in the terminal, workers will catch and ignore the
    KeyboardInterrupt, allowing them to continue processing while the main process
    coordinates a graceful shutdown. This prevents data loss and ensures all resources
    are properly cleaned up.

    Attributes:
        queue_maxsize (int, optional): Maximum size of the communication queues
        err_maxsize (int): Maximum size for error data
        frame_factory (Callable, optional): Function to create Frame objects
        at_eos (bool): Flag indicating if End-Of-Stream has been reached
        _use_threading_override (bool, optional): Set to True to use threading or
            False to use multiprocessing. If not specified, uses the
            Parallelize.use_threading_default

    Example with default process mode:
        @dataclass
        class MyDataSourceElement(ParallelizeSourceElement):
            def __post_init__(self):
                super().__post_init__()
                # Dictionary to track EOS status for each pad
                self.pad_eos = {pad.name: False for pad in self.source_pads}

            def new(self, pad):
                # Check if this pad has already reached EOS
                if self.pad_eos[pad.name]:
                    return Frame(data=None, EOS=True)

                try:
                    # Get data generated by the worker
                    # In a real implementation, you might use pad-specific queues
                    # or have the worker send pad-specific data
                    data = self.out_queue.get(timeout=1)

                    # Check for EOS signal (None typically indicates EOS)
                    if data is None:
                        self.pad_eos[pad.name] = True
                        # If all pads have reached EOS, set global EOS flag
                        if all(self.pad_eos.values()):
                            self.at_eos = True
                        return Frame(data=None, EOS=True)

                    # For data intended for other pads, you might implement
                    # custom routing logic here

                    return Frame(data=data)
                except queue.Empty:
                    # Return an empty frame if no data is available
                    return Frame(data=None)

            def worker_process(self, context: WorkerContext):
                # Generate data and send it back to the main process/thread
                for i in range(10):
                    if context.should_stop():
                        break
                    context.output_queue.put(f"Generated data {i}")
                    time.sleep(0.5)

                # Signal end of stream with None
                context.output_queue.put(None)

                # Wait for worker_stop before terminating
                # This prevents "worker stopped before EOS" errors
                while not context.should_stop():
                    time.sleep(0.1)

    Example with thread mode:
        @dataclass
        class MyThreadedSourceElement(ParallelizeSourceElement):
            _use_threading_override = True

            def __post_init__(self):
                super().__post_init__()
                # Dictionary to track EOS status for each pad
                self.pad_eos = {pad.name: False for pad in self.source_pads}

            def new(self, pad):
                # Similar implementation as in the process mode example,
                # but might use threading-specific features if needed
                if self.pad_eos[pad.name]:
                    return Frame(data=None, EOS=True)

                # Rest of implementation same as the process mode example
    """

    frame_factory: Callable = Frame
    at_eos: bool = False

    def __post_init__(self):
        SourceElement.__post_init__(self)
        _ParallelizeBase.__post_init__(self)
