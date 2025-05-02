# Using Subprocesses in SGN

SGN provides support for running parts of your data processing pipeline in separate processes through the `subprocess` module. This is useful for:

1. CPU-intensive operations that can benefit from parallelization
2. Operations that may block or have unpredictable timing
3. Isolating parts of the pipeline for fault tolerance
4. Utilizing multiple cores efficiently

This tutorial will guide you through creating elements that run in separate processes and share data between them.

## Basic Concepts

The `subprocess` module in SGN provides several key components:

- `SubProcess`: A context manager for running SGN pipelines with elements that implement separate processes. It manages the lifecycle of subprocesses in an SGN pipeline, handling process creation, execution, and cleanup.
- `SubProcessTransformElement`: A Transform element that runs processing logic in a separate process. It communicates with the main process through input and output queues.
- `SubProcessSinkElement`: A Sink element that runs data consumption logic in a separate process. Like the Transform element, it uses queues for communication.
- Shared memory management for efficient data sharing between processes without serialization overhead.

## Creating a Pipeline with Subprocesses

Let's build a simple pipeline that demonstrates how to use the subprocess capabilities:

```python
#!/usr/bin/env python3

from __future__ import annotations
from dataclasses import dataclass
import time
import numpy
from queue import Empty
from sgn.sources import SignalEOS
from sgn.subprocess import SubProcess, SubProcessTransformElement, SubProcessSinkElement
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
class ProcessingTransformElement(SubProcessTransformElement):
    def __post_init__(self):
        super().__post_init__()
        assert len(self.sink_pad_names) == 1 and len(self.source_pad_names) == 1

    def pull(self, pad, frame):
        # Send the frame to the subprocess
        self.in_queue.put(frame)

    @staticmethod
    def sub_process_internal(**kwargs):
        """
        This method runs in a separate process.
        
        The method receives all necessary resources via kwargs, making it more likely 
        to pickle correctly when creating the subprocess.
        
        Args:
            shm_list (list): List of shared memory objects created with SubProcess.to_shm()
            inq (multiprocessing.Queue): Input queue for receiving data from the main process
            outq (multiprocessing.Queue): Output queue for sending data back to the main process
            process_stop (multiprocessing.Event): Event that signals when the process should stop
            process_shutdown (multiprocessing.Event): Event that signals orderly shutdown (process all pending data)
            process_argdict (dict, optional): Dictionary of additional user-specific arguments
        """
        # Extract the kwargs for convenience
        inq, outq = kwargs["inq"], kwargs["outq"]
        process_stop = kwargs["process_stop"]
        
        print(f"Transform subprocess started, process ID: {os.getpid()}")
        while not process_stop.is_set():
            try:
                # Get the next frame with a timeout
                frame = inq.get(timeout=1)
                
                # Process the data (in this case, square the number)
                if frame.data is not None:
                    frame.data = frame.data ** 2
                    print(f"Transform: {frame.data}")
                
                # Send the processed frame back
                outq.put(frame)
                
            except Empty:
                # No data available, just continue
                pass
            
        # Check if we're stopping due to an exception in the main thread
        if kwargs.get("process_shutdown", None) and kwargs["process_shutdown"].is_set():
            print("Main thread is shutting down, cleaning up...")
            # Clean up any remaining items in the queue
            while not inq.empty():
                inq.get_nowait()
        
        # Send EOS when stopping
        outq.put(Frame(EOS=True))
        outq.close()

    def new(self, pad):
        # Get the processed frame from the subprocess
        return self.out_queue.get()

# A Sink element that runs in a separate process
@dataclass
class LoggingSinkElement(SubProcessSinkElement):
    def __post_init__(self):
        super().__post_init__()

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        # Send the frame to the subprocess
        self.in_queue.put((pad.name, frame))

    @staticmethod
    def sub_process_internal(**kwargs):
        """
        This method runs in a separate process.
        
        Args:
            shm_list (list): List of shared memory objects created with SubProcess.to_shm()
            inq (multiprocessing.Queue): Input queue for receiving data from the main process
            outq (multiprocessing.Queue): Output queue for sending data back to the main process
            process_stop (multiprocessing.Event): Event that signals when the process should stop
            process_shutdown (multiprocessing.Event): Event that signals orderly shutdown
            process_argdict (dict, optional): Dictionary of additional user-specific arguments
        """
        import os
        print(f"Sink subprocess started, process ID: {os.getpid()}")
        
        inq, outq = kwargs["inq"], kwargs["outq"]
        process_stop = kwargs["process_stop"]
        while not process_stop.is_set():
            try:
                # Get the next frame with a timeout
                pad_name, frame = inq.get(timeout=1)
                
                # Log the data
                if frame.data is not None:
                    print(f"Sink received on {pad_name}: {frame.data}")
                
                if frame.EOS:
                    print(f"Sink received EOS on {pad_name}")
                
            except Empty:
                # No data available, just continue
                pass
                
        # Check if we're stopping due to an exception in the main thread
        if kwargs.get("process_shutdown", None) and kwargs["process_shutdown"].is_set():
            print("Main thread is shutting down, cleaning up...")
            # Clean up any remaining items in the queue
            while not inq.empty():
                inq.get_nowait()

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

    # Run the pipeline with subprocess management
    with SubProcess(pipeline) as subprocess:
        # This will start the processes and run the pipeline
        subprocess.run()
        # When this block exits, all processes will be cleaned up

if __name__ == "__main__":
    import os
    print(f"Main process ID: {os.getpid()}")
    main()
```

## Sharing Memory Between Processes

For more efficient data sharing, especially with large data structures like NumPy arrays, 
you can use shared memory. The `to_shm()` method creates a shared memory segment that will be 
automatically cleaned up when the SubProcess context manager exits.

```python
# Create shared data in the main process
import numpy as np
from sgn.subprocess import SubProcess

# Create a numpy array and get its byte representation
array_data = np.array([1, 2, 3, 4, 5], dtype=np.float64)
shared_data = array_data.tobytes()

# Register it with SGN's shared memory manager
# This creates a shared memory segment that will be automatically cleaned up
# when the SubProcess context manager exits
# SubProcess.to_shm("my_shared_array", shared_data)
```

Then in your subprocess:

```python
@staticmethod
def sub_process_internal(
    shm_list, inq, outq, process_stop, main_thread_exception, argdict
):
    import numpy as np
    
    # Find our shared memory object
    for item in shm_list:
        if item["name"] == "my_shared_array":
            # Convert the shared memory buffer back to a numpy array
            buffer = item["shm"].buf
            array = np.frombuffer(buffer, dtype=np.float64)
            
            # Now you can use the array
            print(f"Shared array: {array}")
            
            # You can also modify it (changes will be visible to all processes)
            array += 1
            print(f"Modified array: {array}")
```

## Orderly Shutdown and Handling Exceptions

The `SubProcessTransformElement` and `SubProcessSinkElement` classes provide the `sub_process_shutdown()` 
method for initiating an orderly shutdown of a subprocess. This method signals the subprocess to 
complete processing of any pending data and then terminate. It waits for the subprocess to indicate 
completion and collects any remaining data from the output queue.

When either an orderly shutdown is requested or an exception occurs in the main thread, 
the `process_shutdown` event will be set. This allows subprocesses to perform cleanup 
operations before terminating:

```python
@staticmethod
def sub_process_internal(**kwargs):
    inq, outq = kwargs["inq"], kwargs["outq"]
    process_stop = kwargs["process_stop"]
    process_shutdown = kwargs["process_shutdown"]

    while not process_stop.is_set():
        # Normal processing...
        pass
        
    # Check if we're stopping due to an orderly shutdown or exception
    if process_shutdown.is_set():
        print("Processing remaining items and cleaning up...")
        # Process any remaining items in the queue
        while not inq.empty():
            try:
                item = inq.get_nowait()
                # Process the final items...
            except:
                pass
                
    # Always clean up resources
    while not outq.empty():
        outq.get_nowait()
    outq.close()
```

You can also implement graceful shutdown in your element's `pull` method:

```python
def pull(self, pad, frame):
    self.in_queue.put(frame)
    
    if frame.EOS and not self.terminated.is_set():
        # Initiate orderly shutdown and wait up to 10 seconds
        remaining_items = self.sub_process_shutdown(10)
        # Process remaining items if needed
```

## Complete Example with NumPy Array Processing

Here's a more complete example showing how to process NumPy arrays in a subprocess:

```python
#!/usr/bin/env python3

from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import time
from queue import Empty
import os
from sgn.sources import SignalEOS
from sgn.subprocess import SubProcess, SubProcessTransformElement, SubProcessSinkElement
from sgn.base import SourceElement, Frame
from sgn.apps import Pipeline

# Source that generates NumPy arrays
class ArraySourceElement(SourceElement, SignalEOS):
    def __post_init__(self):
        super().__post_init__()
        self.counter = 0
        
    def new(self, pad):
        self.counter += 1
        # Create a random array
        data = np.random.rand(5)
        print(f"Source: Generated array {self.counter}: {data}")
        
        # Stop after generating 5 arrays
        return Frame(data=data, EOS=self.counter >= 5)

# Transform that processes arrays in a subprocess
@dataclass
class ArrayProcessingElement(SubProcessTransformElement):
    def __post_init__(self):
        super().__post_init__()
        assert len(self.sink_pad_names) == 1 and len(self.source_pad_names) == 1

    def pull(self, pad, frame):
        # Send the frame to the subprocess
        self.in_queue.put(frame)

    @staticmethod
    def sub_process_internal(**kwargs):
        print(f"Transform subprocess started, process ID: {os.getpid()}")
        
        inq, outq = kwargs["inq"], kwargs["outq"]
        process_stop = kwargs["process_stop"]
        while not process_stop.is_set():
            try:
                frame = inq.get(timeout=1)
                
                if frame.data is not None:
                    # Process the NumPy array (compute the square)
                    result = frame.data ** 2
                    print(f"Transform: Processed array: {result}")
                    frame.data = result
                
                outq.put(frame)
                
            except Empty:
                pass
                
        # Handle main thread exception
        if main_thread_exception.is_set():
            print("Main thread had an exception, cleaning up...")
            while not inq.empty():
                inq.get_nowait()
        
        outq.put(Frame(EOS=True))
        outq.close()

    def new(self, pad):
        return self.out_queue.get()

# Sink that logs the arrays
@dataclass
class ArraySinkElement(SubProcessSinkElement):
    def __post_init__(self):
        super().__post_init__()

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        self.in_queue.put((pad.name, frame))

    @staticmethod
    def sub_process_internal(**kwargs):
        print(f"Sink subprocess started, process ID: {os.getpid()}")
        
        inq, outq = kwargs["inq"], kwargs["outq"]
        process_stop = kwargs["process_stop"]
        while not process_stop.is_set():
            try:
                pad_name, frame = inq.get(timeout=1)
                
                if frame.data is not None:
                    # Sum the array elements
                    total = frame.data.sum()
                    print(f"Sink: Received array on {pad_name}, sum: {total}")
                
                if frame.EOS:
                    print(f"Sink: Received EOS on {pad_name}")
                
            except Empty:
                pass
                
        if main_thread_exception.is_set():
            print("Main thread had an exception, cleaning up...")
            while not inq.empty():
                inq.get_nowait()

def main():
    # Create the pipeline elements
    source = ArraySourceElement(source_pad_names=("arrays",))
    transform = ArrayProcessingElement(
        sink_pad_names=("input",), source_pad_names=("output",)
    )
    sink = ArraySinkElement(sink_pad_names=("result",))

    # Create the pipeline
    pipeline = Pipeline()

    # Insert the elements and link them
    pipeline.insert(
        source,
        transform,
        sink,
        link_map={
            transform.snks["input"]: source.srcs["arrays"],
            sink.snks["result"]: transform.srcs["output"],
        },
    )

    # Run the pipeline with subprocess management
    with SubProcess(pipeline) as subprocess:
        subprocess.run()

if __name__ == "__main__":
    print(f"Main process ID: {os.getpid()}")
    main()
```

## Best Practices

1. **Clean Queue Management**: Always ensure queues are properly emptied when shutting down, especially when handling exceptions. The `_drainqs()` helper method is available to clean up queues during termination.

2. **Shared Memory**: When working with large data, use `SubProcess.to_shm()` to efficiently share memory between processes rather than passing large objects through queues.

3. **Orderly Shutdown**: Use the `sub_process_shutdown()` method for graceful termination, allowing processes to finish any pending work before stopping.

4. **Exception Handling**: Implement proper exception handling in both the main thread and subprocesses. Check for `process_shutdown` events to properly clean up resources.

5. **Resource Management**: Always close all resources (files, connections, etc.) in your subprocesses before termination. This prevents resource leaks.

6. **Timeouts**: Always use timeouts when getting data from queues to avoid deadlocks. The standard pattern is to use a 1-second timeout and catch Empty exceptions.

7. **Pickling Considerations**: The design of `sub_process_internal` intentionally avoids class or instance references to prevent pickling issues. Pass all data via function arguments.

## Conclusion

The subprocess functionality in SGN provides a powerful way to parallelize data processing across multiple processes. By using the patterns shown in this tutorial, you can create efficient, fault-tolerant pipelines that take advantage of multiple CPU cores.

The careful management of process lifecycle, shared memory, and inter-process communication enables building reliable multi-process pipelines even for complex data processing tasks.
