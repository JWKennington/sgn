## Using Subprocesses and Threads in SGN

SGN provides support for running parts of your data processing pipeline in separate processes or threads through the `subprocess` module. This is useful for:

1. CPU-intensive operations that can benefit from parallelization (using processes)
2. I/O-bound operations that can benefit from concurrency (using threads)
3. Operations that may block or have unpredictable timing
4. Isolating parts of the pipeline for fault tolerance
5. Utilizing multiple cores efficiently (with processes)

This tutorial will guide you through creating elements that run in separate processes or threads and share data between them.

## Basic Concepts

The `subprocess` module in SGN provides several key components:

- `Parallelize`: A context manager for running SGN pipelines with elements that implement separate processes or threads. It manages the lifecycle of workers in an SGN pipeline, handling worker creation, execution, and cleanup.
- `ParallelizeSourceElement`: A Source element that generates data in a separate worker (process or thread). It communicates with the main process/thread through an output queue.
- `ParallelizeTransformElement`: A Transform element that runs processing logic in a separate worker (process or thread). It communicates with the main process/thread through input and output queues.
- `ParallelizeSinkElement`: A Sink element that runs data consumption logic in a separate worker. Like the Transform element, it uses queues for communication.
- Shared memory management for efficient data sharing between processes (not needed for threads, which share memory by default).

> **IMPORTANT**: When using process mode, code using the `Parallelize` context manager must be wrapped within an `if __name__ == "__main__":` block. This requirement exists because SGN uses Python's multiprocessing module with the 'spawn' start method, which requires that the main module be importable.

## Threading vs Multiprocessing

SGN supports both threading and multiprocessing concurrency models. You can choose which model to use at both the pipeline level and the individual element level.

### When to Use Threading

- For I/O-bound operations (network requests, file I/O)
- When sharing large data objects (no serialization overhead)
- Lower overhead for creation and communication
- Good for tasks where GIL contention is not an issue

### When to Use Multiprocessing

- For CPU-bound operations (computation, data processing)
- To bypass the Global Interpreter Lock (GIL)
- For true parallel execution
- When isolation between elements is needed
- When running on multiple CPU cores is beneficial

### Setting the Concurrency Mode

#### Default Mode for All Elements

You can set the default mode for all elements by setting the `Parallelize.use_threading_default` class attribute:

```{.python notest}
# Example (pseudocode)
from sgn.subprocess import Parallelize

# Set the default mode to threading for all elements
Parallelize.use_threading_default = True
```

#### Mode for a Specific Pipeline

You can specify the concurrency mode for a specific pipeline using the `use_threading` parameter:

```{.python notest}
# Example (pseudocode)
from sgn.subprocess import Parallelize
from sgn.apps import Pipeline

# Assume pipeline is already created
pipeline = Pipeline()

# Use process mode (default)
with Parallelize(pipeline) as parallelize:
    parallelize.run()

# Use thread mode
with Parallelize(pipeline, use_threading=True) as parallelize:
    parallelize.run()
```

#### Mode for Individual Elements

You can specify the concurrency mode for individual elements by setting the `_use_threading_override` class variable:

```python
from dataclasses import dataclass
from sgn.subprocess import ParallelizeTransformElement

@dataclass
class MyThreadedTransform(ParallelizeTransformElement):
    # Override the default to use thread mode for this element
    _use_threading_override = True

    # ... rest of implementation
```

## Implementing ParallelizeSourceElement

The `ParallelizeSourceElement` class now requires implementing the abstract `new()` method. This method is responsible for retrieving data from the worker process/thread and creating frames for each source pad.

Here's a simple pattern for implementing a source element:

```python
from dataclasses import dataclass
from queue import Empty
from sgn.base import Frame
from sgn.subprocess import ParallelizeSourceElement

@dataclass
class MySourceElement(ParallelizeSourceElement):
    # Optional: override default concurrency mode
    _use_threading_override = True  # Use threading instead of multiprocessing
    item_count: int = 5  # Total items to generate

    def __post_init__(self):
        super().__post_init__()
        # Initialize counter
        self.counter = 0
        # Initialize EOS flag
        self.at_eos = False
        # Pass parameters to worker
        self.worker_argdict = {"item_count": self.item_count}

    def internal(self):
        """
        Called by the pipeline to generate data.

        This method is responsible for sending commands to the worker.
        """
        # Only send new count if we haven't reached the limit
        if self.counter < self.item_count and not self.at_eos:
            self.counter += 1
            self.in_queue.put(self.counter)

    def new(self, pad):
        """Get the next frame for the given pad."""
        # If we're at EOS, keep returning EOS frames
        if self.at_eos:
            return Frame(data=None, EOS=True)

        try:
            # Try to get data from the queue with a short timeout
            data = self.out_queue.get(timeout=0.1)

            # None signals EOS
            if data is None:
                self.at_eos = True
                return Frame(data=None, EOS=True)

            # Return regular data frame
            return Frame(data=data)

        except Empty:
            # If queue is empty, return empty frame
            return Frame(data=None)

    @staticmethod
    def sub_process_internal(**kwargs):
        """
        This method runs in a separate process/thread to handle data.

        It reads count values from the input queue, processes them,
        and puts the results in the output queue.
        """
        inq = kwargs["inq"]
        outq = kwargs["outq"]
        worker_stop = kwargs["worker_stop"]

        try:
            # Check if we should stop
            if worker_stop.is_set():
                return

            # Get count from input queue (non-blocking)
            if not inq.empty():
                count = inq.get_nowait()

                # If count exceeds limit, send EOS
                if count >= kwargs.get("worker_argdict", {}).get("item_count", 5):
                    outq.put(None)  # Signal EOS
                else:
                    # Process the count and send result
                    result = f"Processed item {count}"
                    outq.put(result)
        except Empty:
            # Queue is empty, do nothing this time
            pass
```

## Creating a Pipeline with Worker Elements

Let's build a simple pipeline that demonstrates how to use elements with different concurrency models:

```python
#!/usr/bin/env python3

from __future__ import annotations
from dataclasses import dataclass
import time
from queue import Empty
from sgn.sources import SignalEOS
from sgn.subprocess import Parallelize, ParallelizeTransformElement, ParallelizeSinkElement
from sgn.base import SourceElement, Frame
from sgn.apps import Pipeline

# A simple source class that generates sequential numbers
class NumberSourceElement(SourceElement, SignalEOS):
    def __post_init__(self):
        super().__post_init__()
        self.counter = 0

    def new(self, pad):
        self.counter += 1
        # Stop after generating 10 numbers
        return Frame(data=self.counter, EOS=self.counter >= 10)

# A Transform element that runs in a separate process
@dataclass
class ProcessingTransformElement(ParallelizeTransformElement):
    def __post_init__(self):
        super().__post_init__()
        assert len(self.sink_pad_names) == 1 and len(self.source_pad_names) == 1

    def pull(self, pad, frame):
        # Send the frame to the worker
        self.in_queue.put(frame)
        if frame.EOS and not self.terminated.is_set():
            self.at_eos = True
            # Signal shutdown but don't rely on frame_list
            self.sub_process_shutdown(10)

    @staticmethod
    def sub_process_internal(**kwargs):
        """
        This method runs in a separate process or thread.

        The method receives all necessary resources via kwargs, making it more likely
        to pickle correctly when creating the worker.

        Args:
            shm_list (list): List of shared memory objects created with SubProcess.to_shm()
                            (only relevant for process mode)
            inq (Queue): Input queue for receiving data from the main process/thread
            outq (Queue): Output queue for sending data back to the main process/thread
            worker_stop (Event): Event that signals when the worker should stop
            worker_shutdown (Event): Event that signals orderly shutdown (process all pending data)
            worker_argdict (dict, optional): Dictionary of additional user-specific arguments
        """
        # Extract the kwargs for convenience
        inq, outq = kwargs["inq"], kwargs["outq"]
        worker_stop = kwargs["worker_stop"]

        import os
        print(f"Transform worker started, process ID: {os.getpid()}")
        try:
            # Get the next frame with a timeout
            frame = inq.get(timeout=0.1)

            # Process the data (in this case, square the number)
            if frame.data is not None:
                frame.data = frame.data ** 2
                print(f"Transform: {frame.data}")

            # Send the processed frame back
            outq.put(frame)

        except Empty:
            # No data available, just continue
            pass

    def new(self, pad):
        # Simply get the processed frame from the worker queue
        try:
            return self.out_queue.get(timeout=0.1)
        except Empty:
            # Return empty frame if no data is available
            return Frame(data=None)

# A Sink element that runs in a separate thread
@dataclass
class LoggingSinkElement(ParallelizeSinkElement):
    # Use thread mode for this element
    _use_threading_override = True

    def __post_init__(self):
        super().__post_init__()
        # Track how many pads have reached EOS
        self.eos_count = 0
        self.expected_eos_count = len(self.sink_pad_names)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            # Increment EOS count for this pad
            self.eos_count += 1
            # Track when all pads have reached EOS
            if self.eos_count >= self.expected_eos_count and not self.terminated.is_set():
                self.sub_process_shutdown(10)
        # Send the frame to the worker
        self.in_queue.put((pad.name, frame))

    @staticmethod
    def sub_process_internal(**kwargs):
        """
        This method runs in a separate thread.

        Args:
            inq (Queue): Input queue for receiving data from the main process/thread
            outq (Queue): Output queue for sending data back to the main process/thread
            worker_stop (Event): Event that signals when the worker should stop
            worker_shutdown (Event): Event that signals orderly shutdown
            worker_argdict (dict, optional): Dictionary of additional user-specific arguments
        """
        import os
        print(f"Sink worker started, process ID: {os.getpid()}")

        inq = kwargs["inq"]
        worker_stop = kwargs["worker_stop"]

        # Only process if not stopped
        if worker_stop.is_set():
            return

        try:
            # Get the next frame with a timeout
            pad_name, frame = inq.get(timeout=0.1)

            # Log the data
            if frame and not frame.EOS and frame.data is not None:
                print(f"Sink received on {pad_name}: {frame.data}")

            if frame and frame.EOS:
                print(f"Sink received EOS on {pad_name}")

        except Empty:
            # No data available, just continue
            pass

def main():
    # Create the pipeline elements
    source = NumberSourceElement(source_pad_names=("numbers",))
    transform = ProcessingTransformElement(
        sink_pad_names=("input",), source_pad_names=("output",)
    )
    sink = LoggingSinkElement(sink_pad_names=("result",))

    # Create the pipeline
    pipeline = Pipeline()

    # Insert the elements and link them
    pipeline.insert(
        source,
        transform,
        sink,
        link_map={
            transform.snks["input"]: source.srcs["numbers"],
            sink.snks["result"]: transform.srcs["output"],
        },
    )

    # Run the pipeline with worker management
    # We'll use threads as the default mode, but the transform element will still
    # use processes because it doesn't override the default
    with Parallelize(pipeline, use_threading=True) as parallelize:
        # This will start the workers and run the pipeline
        parallelize.run()
        # When this block exits, all workers will be cleaned up

if __name__ == "__main__":
    import os
    print(f"Main process ID: {os.getpid()}")
    main()
```

## Sharing Memory Between Processes

For more efficient data sharing when using processes (not needed for threads), especially with large data structures like NumPy arrays, you can use shared memory. The `to_shm()` method creates a shared memory segment that will be automatically cleaned up when the Parallelize context manager exits.

```{.python notest}
# Create shared data in the main process
import numpy as np
from sgn.subprocess import Parallelize

# Create a numpy array and get its byte representation
array_data = np.array([1, 2, 3, 4, 5], dtype=np.float64)
shared_data = array_data.tobytes()

# Register it with SGN's shared memory manager
# This creates a shared memory segment that will be automatically cleaned up
# when the Parallelize context manager exits
shm_ref = Parallelize.to_shm("shared_array_example", shared_data)
```

Then in your process worker:

```{.python notest}
@staticmethod
def sub_process_internal(**kwargs):
    import numpy as np

    # Find our shared memory object
    for item in kwargs["shm_list"]:
        if item["name"] == "shared_array_example":
            # Convert the shared memory buffer back to a numpy array
            buffer = item["shm"].buf
            array = np.frombuffer(buffer, dtype=np.float64)

            # Now you can use the array
            print(f"Shared array: {array}")

            # You can also modify it (changes will be visible to all processes)
            array += 1
            print(f"Modified array: {array}")
```

> **Note**: Shared memory is only needed when using processes. Threads automatically share the same memory space, so you can directly share objects between threads without using `to_shm()`.

## Orderly Shutdown and Handling Exceptions

The `ParallelizeTransformElement` and `ParallelizeSinkElement` classes provide the `sub_process_shutdown()` method for initiating an orderly shutdown of a worker. This method signals the worker to complete processing of any pending data and then terminate. It waits for the worker to indicate completion and collects any remaining data from the output queue.

When either an orderly shutdown is requested or an exception occurs in the main thread, the `worker_shutdown` event will be set. This allows workers to perform cleanup operations before terminating:

```{.python notest}
@staticmethod
def sub_process_internal(**kwargs):
    inq, outq = kwargs["inq"], kwargs["outq"]
    worker_stop = kwargs["worker_stop"]
    worker_shutdown = kwargs["worker_shutdown"]

    # IMPORTANT: Don't use your own infinite loop here!
    # This method is already called repeatedly by the framework
    # Each call should just process one unit of work

    # Check if we should stop
    if worker_stop.is_set():
        return

    # Check if we're in orderly shutdown mode
    if worker_shutdown and worker_shutdown.is_set():
        # Process any remaining item in the queue
        if not inq.empty():
            try:
                item = inq.get_nowait()
                # Process this final item...
                result = process_item(item)
                outq.put(result)
            except Exception:
                pass
        return

    # Normal processing for a single item
    try:
        item = inq.get(timeout=0.1)
        # Process the item...
        result = process_item(item)
        outq.put(result)
    except Empty:
        # No data available
        pass
```

You can also implement graceful shutdown in your element's `pull` method:

```{.python notest}
def pull(self, pad, frame):
    # Send frame to worker if it exists
    if self.in_queue is not None:
        self.in_queue.put(frame)

    # Check for EOS condition
    if frame.EOS:
        self.mark_eos(pad)
        # Track if all pads have reached EOS
        if self.at_eos and not self.terminated.is_set():
            # Initiate orderly shutdown and wait up to 10 seconds
            self.sub_process_shutdown(10)
```

## Signal Handling in Workers

SGN's subprocess implementation includes special handling for signals, particularly `KeyboardInterrupt` (Ctrl+C). When you press Ctrl+C in a terminal running an SGN pipeline with workers, the behavior is designed to ensure a clean, coordinated shutdown:

1. **KeyboardInterrupt Resilience**: Workers automatically catch and ignore `KeyboardInterrupt` exceptions. This prevents workers from terminating abruptly when Ctrl+C is pressed.

2. **Coordinated Shutdown**: Instead of individual workers terminating independently, the main process receives the signal and coordinates a graceful shutdown of all worker elements.

3. **Continued Processing**: While the shutdown sequence is in progress, workers will continue processing their current tasks and can drain any remaining items in their queues.

This design provides several benefits:

- Prevents data loss during processing
- Ensures all workers shut down in a coordinated manner
- Allows for proper cleanup of resources like shared memory and queues
- Creates more predictable pipeline behavior

## Implementation Considerations

When deciding between threading and multiprocessing, consider these factors:

1. **Thread Safety**: When using thread mode, be mindful of thread safety in your code. Processes provide natural isolation, but threads share the same memory space.

2. **GIL Constraints**: The Global Interpreter Lock (GIL) in Python prevents true parallel execution of Python code in threads. For CPU-bound tasks, processes will generally perform better.

3. **Shared Memory**: When using process mode, use `SubProcess.to_shm()` for efficient data sharing. In thread mode, direct memory sharing is already available.

4. **Serialization**: Process mode requires data to be serialized when passing through queues (unless using shared memory). Thread mode doesn't have this overhead.

5. **Creation Overhead**: Creating processes is more expensive than creating threads. For short-lived tasks, thread mode may be more efficient.

6. **Queue Performance**: Thread queues are generally faster than process queues due to not requiring serialization.

## Best Practices

1. **Main Guard Pattern**: When using process mode, always wrap your main code inside an `if __name__ == "__main__":` block. This is critical because SGN uses the 'spawn' multiprocessing start method, which requires that the main module be importable.

    ```python
    def main():
        # Create your pipeline...
        with Parallelize(pipeline) as parallelize:
            parallelize.run()

    if __name__ == "__main__":
        main()
    ```

2. **Choose the Right Concurrency Model**:
   - Use processes for CPU-bound tasks that need to bypass the GIL
   - Use threads for I/O-bound tasks or when sharing large objects without serialization

3. **Clean Queue Management**: Always ensure queues are properly emptied when shutting down, especially when handling exceptions. The `_drainqs()` helper method is available to clean up queues during termination.

4. **Shared Memory**: When working with large data in process mode, use `Parallelize.to_shm()` to efficiently share memory between processes rather than passing large objects through queues.

5. **Orderly Shutdown**: Use the `sub_process_shutdown()` method for graceful termination, allowing workers to finish any pending work before stopping.

6. **Signal Handling**: Be aware that workers will ignore `KeyboardInterrupt` signals. If you need custom signal handling in your workers, implement it in your `sub_process_internal` method, but make sure you don't interfere with SGN's ability to coordinate worker shutdown.

7. **Exception Handling**: Implement proper exception handling in both the main thread and workers. Check for `worker_shutdown` events to properly clean up resources.

8. **Resource Management**: Always close all resources (files, connections, etc.) in your workers before termination. This prevents resource leaks.

9. **Timeouts**: Always use timeouts when getting data from queues to avoid deadlocks. The standard pattern is to use a short timeout (0.1 to 1 second) and catch Empty exceptions.

10. **Pickling Considerations**: When using process mode, the design of `sub_process_internal` intentionally avoids class or instance references to prevent pickling issues. Pass all data via function arguments. Avoid using lambda functions in multiprocessing code, as they cannot be properly pickled.

11. **Implement the abstract `new()` method**: When creating a `ParallelizeSourceElement` subclass, you must implement the abstract `new()` method to retrieve data from the worker and create frames for each source pad.

## Conclusion

The subprocess functionality in SGN provides a powerful way to utilize both threading and multiprocessing concurrency models in your data pipelines. By choosing the appropriate concurrency model for each element based on its specific needs, you can create efficient, fault-tolerant pipelines that maximize performance.

The ability to mix thread-based and process-based elements in the same pipeline gives you the flexibility to choose the right tool for each job - threads for I/O-bound operations and processes for CPU-bound operations. This approach allows you to build sophisticated pipelines that can take advantage of both concurrency models within a single application.