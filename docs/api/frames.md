# Frames - Data Flow Primitives

Frames are the fundamental units of data that flow through SGN pipelines. Every piece of information moving between elements is wrapped in a `Frame` object.

## Overview

SGN provides three frame types:

1. **Frame** - General-purpose container for any data type
2. **IterFrame** - Specialized frame for iterable data (lists, arrays, etc.)
3. **DataSpec** - Type specification for frame data validation

## Quick Start: Creating Frames

### Basic Frame

The simplest frame just wraps data:

```python
from sgn.frames import Frame

# Create a frame with data
frame = Frame(data=42)
print(frame.data)  # 42

# Frame with string data
text_frame = Frame(data="Hello SGN")
print(text_frame.data)  # Hello SGN

# Frame with dict data
dict_frame = Frame(data={"key": "value", "count": 10})
print(dict_frame.data)  # {'key': 'value', 'count': 10}
```

### End-of-Stream (EOS) Frames

EOS frames signal that no more data will arrive:

```python
from sgn.frames import Frame

# Regular data frame
data_frame = Frame(data=100)
print(data_frame.EOS)  # False

# End-of-stream frame
eos_frame = Frame(EOS=True)
print(eos_frame.EOS)  # True
print(eos_frame.data)  # None
```

### Gap Frames

Gap frames indicate missing or invalid data:

```python
from sgn.frames import Frame

# Normal frame
normal = Frame(data=50)
print(normal.is_gap)  # False

# Gap frame (data is present but marked as gap)
gap = Frame(data=None, is_gap=True)
print(gap.is_gap)  # True
```

## Frame Attributes

Every frame has five attributes:

```python
from sgn.frames import Frame, DataSpec

frame = Frame(
    data=100,              # The actual payload
    EOS=False,             # End-of-stream flag
    is_gap=False,          # Gap marker
    spec=DataSpec(),       # Data specification
    metadata={"src": "sensor1"}  # Additional metadata
)

print(f"Data: {frame.data}")
print(f"EOS: {frame.EOS}")
print(f"Gap: {frame.is_gap}")
print(f"Spec: {frame.spec}")
print(f"Metadata: {frame.metadata}")
```

## Using Frames in Pipelines

### Source Element Creating Frames

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class SensorSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["readings"], **kwargs)
        self.count = 0

    def new(self, pad):
        self.count += 1

        # Send 5 readings then EOS
        if self.count > 5:
            return Frame(EOS=True)

        # Create frame with sensor reading
        return Frame(
            data=self.count * 10,
            metadata={"sensor": "temp", "unit": "celsius"}
        )

class PrinterSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["data"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            print("Stream ended")
            return

        print(f"Reading: {frame.data} {frame.metadata['unit']}")

p = Pipeline()
p.insert(SensorSource(), PrinterSink())
p.link({p.elements[1].snks["data"]: p.elements[0].srcs["readings"]})
p.run()
# Output:
# Reading: 10 celsius
# Reading: 20 celsius
# Reading: 30 celsius
# Reading: 40 celsius
# Reading: 50 celsius
# Stream ended
```

## Frame Metadata

Metadata allows you to attach additional information to frames:

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
import time

class TimestampedSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.n = 0

    def new(self, pad):
        self.n += 1
        if self.n > 3:
            return Frame(EOS=True)

        return Frame(
            data=self.n,
            metadata={
                "timestamp": time.time(),
                "source_id": "sensor_01",
                "quality": "high"
            }
        )

class MetadataPrinter(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return

        print(f"Value: {frame.data}")
        print(f"  Source: {frame.metadata['source_id']}")
        print(f"  Quality: {frame.metadata['quality']}")

p = Pipeline()
p.insert(TimestampedSource(), MetadataPrinter())
p.link({p.elements[1].snks["in"]: p.elements[0].srcs["out"]})
p.run()
```

## IterFrame: Frames with Iterable Data

`IterFrame` is optimized for frames containing sequences:

```python
from sgn.base import SourceElement, SinkElement
from sgn.frames import IterFrame, Frame
from sgn.apps import Pipeline

class BatchSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["batches"], **kwargs)
        self.batch_num = 0

    def new(self, pad):
        self.batch_num += 1
        if self.batch_num > 3:
            return Frame(EOS=True)

        # Create IterFrame with list of values
        return IterFrame(data=[
            self.batch_num * 10 + i
            for i in range(1, 4)
        ])

class BatchPrinter(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return

        print(f"Batch: {list(frame.data)}")

p = Pipeline()
p.insert(BatchSource(), BatchPrinter())
p.link({p.elements[1].snks["in"]: p.elements[0].srcs["batches"]})
p.run()
# Output:
# Batch: [11, 12, 13]
# Batch: [21, 22, 23]
# Batch: [31, 32, 33]
```

## DataSpec: Type Specifications

`DataSpec` ensures data consistency across frames:

```python
from sgn.frames import Frame, DataSpec

# Define a data specification
spec = DataSpec()

# Frames with the same spec
frame1 = Frame(data=100, spec=spec)
frame2 = Frame(data=200, spec=spec)

print(frame1.spec == frame2.spec)  # True

# DataSpec is frozen (immutable)
# spec.new_field = "value"  # This would raise an error
```

!!! info "DataSpec Validation"
    SGN validates that frames passing through a sink pad have consistent DataSpec values. If a frame arrives with a different spec than previous frames, a `ValueError` is raised.

## Common Patterns

### Pattern 1: Simple Data Frames

```python
from sgn.frames import Frame

# Just data
frame = Frame(data=42)

# Data with metadata
frame = Frame(
    data=42,
    metadata={"units": "meters"}
)
```

### Pattern 2: Signaling End-of-Stream

```python
from sgn.base import SourceElement, Frame

class MySource(SourceElement):
    def __init__(self, max_count=10, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.count = 0
        self.max_count = max_count

    def new(self, pad):
        self.count += 1

        # Stop after max_count
        if self.count > self.max_count:
            return Frame(EOS=True)

        return Frame(data=self.count)
```

### Pattern 3: Handling Gaps in Data

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class GappySource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.n = 0

    def new(self, pad):
        self.n += 1
        if self.n > 5:
            return Frame(EOS=True)

        # Mark even numbers as gaps
        if self.n % 2 == 0:
            return Frame(data=None, is_gap=True)

        return Frame(data=self.n)

class GapHandler(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return

        if frame.is_gap:
            print(f"Gap detected, skipping...")
        else:
            print(f"Valid data: {frame.data}")

p = Pipeline()
p.insert(GappySource(), GapHandler())
p.link({p.elements[1].snks["in"]: p.elements[0].srcs["out"]})
p.run()
# Output:
# Valid data: 1
# Gap detected, skipping...
# Valid data: 3
# Gap detected, skipping...
# Valid data: 5
```

## Frame vs IterFrame: When to Use Which?

### Use Frame when:
- Data is a single value (number, string, object)
- Data is a dict or custom object
- You don't need iterable-specific features

### Use IterFrame when:
- Data is inherently a sequence (list, array, etc.)
- You're processing batches of items
- You want to emphasize that data is iterable

```python
from sgn.frames import Frame, IterFrame

# Single values -> Frame
temperature = Frame(data=72.5)
message = Frame(data="Hello")
record = Frame(data={"id": 1, "name": "Alice"})

# Sequences -> IterFrame
measurements = IterFrame(data=[72.5, 73.1, 72.8])
tokens = IterFrame(data=["hello", "world"])
batch = IterFrame(data=range(10))
```

## Best Practices

### 1. Always Handle EOS in Sinks

```python
from sgn.base import SinkElement

class MySink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)  # IMPORTANT: Must mark EOS
            return

        # Process frame.data
```

### 2. Use Metadata for Context

```python
from sgn.frames import Frame
import time

sensor_reading = 72.5
ts = time.time()

# Good: Metadata for auxiliary info
frame = Frame(
    data=sensor_reading,
    metadata={"timestamp": ts, "sensor_id": "A1"}
)

# Avoid: Putting everything in data
frame = Frame(data={"reading": sensor_reading, "ts": ts, "id": "A1"})
```

### 3. Propagate EOS in Transforms

```python
from sgn.base import TransformElement
from sgn.frames import Frame

class MyTransform(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(
            source_pad_names=["out"],
            sink_pad_names=["in"],
            **kwargs
        )
        self.input_frame = None

    def pull(self, pad, frame):
        self.input_frame = frame

    def new(self, pad):
        # Propagate EOS
        if self.input_frame.EOS:
            return Frame(EOS=True)

        # Transform data
        return Frame(data=self.input_frame.data * 2)
```

## Related Tutorials

- [End of Stream](../tutorials/end_of_stream.md) - Detailed guide to EOS handling
- [Hello World](../tutorials/hello_world.md) - Basic frame usage in pipelines

## API Reference

::: sgn.frames
