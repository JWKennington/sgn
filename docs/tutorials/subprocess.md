# Using Subprocesses in SGN

SGN provides support for running parts of your data processing pipeline in separate processes through the `subprocess` module. This is useful for:

1. CPU-intensive operations that can benefit from parallelization
2. Operations that may block or have unpredictable timing
3. Isolating parts of the pipeline for fault tolerance
4. Utilizing multiple cores efficiently

This tutorial will guide you through creating elements that run in separate processes and share data between them.

## Basic Concepts

The `subprocess` module in SGN provides several key components:

- `SubProcess`: A context manager for running SGN pipelines with elements that implement separate processes
- `SubProcessTransformElement`: A Transform element that runs in a separate process
- `SubProcessSinkElement`: A Sink element that runs in a separate process
- Shared memory management for efficient data sharing between processes

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
    def sub_process_internal(
        shm_list, inq, outq, process_stop, main_thread_exception, argdict
    ):
        """
        This method runs in a separate process.
        
        Args:
            shm_list: List of shared memory objects
            inq: Input queue for receiving data
            outq: Output queue for sending data
            process_stop: Event that signals when the process should stop
            main_thread_exception: Event that signals when the main thread has an exception
            argdict: Additional arguments dictionary
        """
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
        if main_thread_exception.is_set():
            print("Main thread had an exception, cleaning up...")
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
    def sub_process_internal(
        shm_list, inq, outq, process_stop, main_thread_exception, argdict
    ):
        """
        This method runs in a separate process.
        """
        import os
        print(f"Sink subprocess started, process ID: {os.getpid()}")
        
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
        if main_thread_exception.is_set():
            print("Main thread had an exception, cleaning up...")
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
you can use shared memory. Here's how to use it:

```python
# Create shared data in the main process
import numpy as np
from sgn.subprocess import SubProcess

# Create a numpy array and get its byte representation
array_data = np.array([1, 2, 3, 4, 5], dtype=np.float64)
shared_data = array_data.tobytes()

# Register it with SGN's shared memory manager
SubProcess.to_shm("my_shared_array", shared_data)
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

## Handling Exceptions

When an exception occurs in the main thread, the `main_thread_exception` event will be set.
This allows subprocesses to perform cleanup operations before terminating:

```python
@staticmethod
def sub_process_internal(
    shm_list, inq, outq, process_stop, main_thread_exception, argdict
):
    while not process_stop.is_set():
        # Normal processing...
        pass
        
    # Check if we're stopping due to an exception
    if main_thread_exception.is_set():
        print("Main thread had an exception, cleaning up...")
        # Perform cleanup (e.g., empty queues, close resources)
        while not outq.empty():
            outq.get_nowait()
    
    outq.close()
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
    def sub_process_internal(
        shm_list, inq, outq, process_stop, main_thread_exception, argdict
    ):
        print(f"Transform subprocess started, process ID: {os.getpid()}")
        
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
    def sub_process_internal(
        shm_list, inq, outq, process_stop, main_thread_exception, argdict
    ):
        print(f"Sink subprocess started, process ID: {os.getpid()}")
        
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

1. **Clean Queue Management**: Always ensure queues are properly emptied when shutting down, especially when handling exceptions.

2. **Shared Memory**: When working with large data, use shared memory rather than passing data through queues.

3. **Exception Handling**: Implement proper exception handling in both the main thread and subprocesses.

4. **Resource Management**: Make sure to close all resources (files, connections, etc.) in your subprocesses.

5. **Timeouts**: Always use timeouts when getting data from queues to avoid deadlocks.

## Conclusion

The subprocess functionality in SGN provides a powerful way to parallelize data processing across multiple processes. By using the patterns shown in this tutorial, you can create efficient, fault-tolerant pipelines that take advantage of multiple CPU cores.