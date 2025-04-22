# Signal Handling with SignalEOS

This tutorial demonstrates how to gracefully handle keyboard interrupts (Ctrl+C) in your SGN pipeline using the `SignalEOS` class from `sgn.sources`.

## The Problem

When you run an SGN pipeline, it typically continues indefinitely until all source elements mark their frames as End-Of-Stream (EOS). However, users often want to terminate a running pipeline by pressing `Ctrl+C`. By default, this sends a SIGINT signal that abruptly terminates the program, which may lead to:

- Incomplete data processing
- Resources not being properly cleaned up
- Temporary files not being deleted
- Output data being corrupted

## The Solution: SignalEOS

SGN provides the `SignalEOS` class which helps handle signals like SIGINT (Ctrl+C) and SIGTERM gracefully. Here's how it works:

1. It captures signals like Ctrl+C
2. Instead of immediately terminating, it marks that a signal was received
3. Your source elements can check if a signal was received and set EOS on their frames
4. This allows the pipeline to shut down cleanly

## Basic Example

Here's a simple example:

```python
#!/usr/bin/env python3

import time
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.sources import SignalEOS

class MySignalAwareSource(SourceElement, SignalEOS):
    def __post_init__(self):
        super().__post_init__()
        self.count = 0
        
    def new(self, pad):
        # Check if we received a signal (like Ctrl+C)
        eos = self.signaled_eos() or self.count > 10
        
        time.sleep(1)
        self.count += 1
        print(f"Producing frame {self.count}")
        
        return Frame(data=f"Frame {self.count}", EOS=eos)

class MySink(SinkElement):
    def pull(self, pad, frame):
        print(f"Processing: {frame.data}")
        
        if frame.EOS:
            print("End of stream received, shutting down...")
            self.mark_eos(pad)

# Create pipeline elements
source = MySignalAwareSource(source_pad_names=("output",))
sink = MySink(sink_pad_names=("input",))

# Set up pipeline
pipeline = Pipeline()
pipeline.insert(source, sink, link_map={sink.snks["input"]: source.srcs["output"]})

# Run inside the SignalEOS context manager
with SignalEOS() as signal_eos:
    print("Pipeline started. Press Ctrl+C to stop gracefully...")
    pipeline.run()

print("Pipeline has shut down cleanly.")
```

## How It Works

1. `MySignalAwareSource` inherits from both `SourceElement` and `SignalEOS`
2. In the `new()` method, it checks `self.signaled_eos()` to see if a signal was received
3. If a signal was received, it sets `EOS=True` on the frame
4. The pipeline is run inside a `with SignalEOS()` context manager, which:
   - Sets up signal handlers for SIGINT and SIGTERM when entering
   - Restores the original signal handlers when exiting
5. When the user presses Ctrl+C, the signal is caught by the SignalEOS handler
6. The next frame from the source will be marked with EOS=True
7. The sink receives the EOS frame and calls `mark_eos()`
8. The pipeline detects that all sinks have received EOS and terminates cleanly

## More Advanced Example

Here's a more realistic example where we might be processing data and want to make sure everything is saved properly before shutting down:

```python
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame, TransformElement
from sgn.apps import Pipeline
from sgn.sources import SignalEOS
import time

class DataGenerator(SourceElement, SignalEOS):
    def __post_init__(self):
        super().__post_init__()
        self.count = 0
        
    def new(self, pad):
        # Simulate some data generation work
        time.sleep(0.5)
        self.count += 1
        
        # Check if we received a signal
        eos = self.signaled_eos() or self.count > 10 
        if eos:
            print("Signal received, preparing to shut down...")
        
        data = f"Data point {self.count}"
        return Frame(data=data, EOS=eos)

class DataProcessor(TransformElement):
    def pull(self, pad, frame):
        # Store the incoming frame for each pad
        setattr(self, f"frame_{pad.name}", frame)
    
    def new(self, pad):
        # Get the corresponding input frame
        frame = getattr(self, f"frame_{self.sink_pads[0].name}")
        
        # Process the data (in this case, just uppercase it)
        if not frame.EOS:
            processed_data = frame.data.upper()
        else:
            processed_data = frame.data
            
        # Pass on the EOS flag
        return Frame(data=processed_data, EOS=frame.EOS)

class DataSaver(SinkElement):
    def __post_init__(self):
        super().__post_init__()
        self.saved_data = []
        
    def pull(self, pad, frame):
        if not frame.EOS:
            # Save the data
            self.saved_data.append(frame.data)
            print(f"Saved: {frame.data}")
        else:
            # Final cleanup when EOS is received
            print("EOS received, finalizing data save...")
            print(f"Total data points saved: {len(self.saved_data)}")
            self.mark_eos(pad)

# Create pipeline elements
source = DataGenerator(source_pad_names=("raw_data",))
processor = DataProcessor(source_pad_names=("processed_data",), sink_pad_names=("raw_data",))
sink = DataSaver(sink_pad_names=("processed_data",))

# Build the pipeline
pipeline = Pipeline()
pipeline.insert(
    source, processor, sink,
    link_map={
        processor.snks["raw_data"]: source.srcs["raw_data"],
        sink.snks["processed_data"]: processor.srcs["processed_data"]
    }
)

# Run with signal handling
print("Starting pipeline. Press Ctrl+C to stop gracefully...")
with SignalEOS() as signal_eos:
    pipeline.run()

print("Pipeline has shut down cleanly.")
```

## Key Points

1. Make your source elements inherit from `SignalEOS` if you want them to be signal-aware
2. Always run your pipeline inside the `SignalEOS` context manager
3. Check `self.signaled_eos()` in your source's `new()` method to detect signals
4. Set `EOS=True` on frames when a signal is detected
5. Properly handle EOS in your sink elements by calling `self.mark_eos(pad)`

By using the `SignalEOS` class, your SGN pipelines can respond gracefully to Ctrl+C, allowing for proper cleanup and shutdown rather than abrupt termination.
