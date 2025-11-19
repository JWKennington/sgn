# Base Classes - Core Building Blocks

The `sgn.base` module provides the fundamental abstractions for building SGN pipelines: **Elements** and **Pads**.

## Overview

SGN pipelines are built from three types of elements connected by pads:

- **SourceElement** - Generates data (e.g., reading from files, sensors, streams)
- **TransformElement** - Processes data (e.g., filtering, mapping, aggregating)
- **SinkElement** - Consumes data (e.g., writing to files, databases, displays)

Elements communicate through **pads**:

- **SourcePad** - Outputs data from an element
- **SinkPad** - Receives data into an element

## Quick Start: Creating Custom Elements

### Creating a Source Element

A source element generates frames of data. You must implement the `new()` method:

```python
from sgn.base import SourceElement, Frame

class MySource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["output"], **kwargs)
        self.counter = 0

    def new(self, pad):
        """Generate a new frame with incrementing counter."""
        self.counter += 1
        return Frame(data=self.counter)

# Create instance
source = MySource(name="counter_source")
```

### Creating a Sink Element

A sink element consumes frames. You must implement the `pull()` method:

```python
from sgn.base import SinkElement, Frame

class MySink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["input"], **kwargs)
        self.received = []

    def pull(self, pad, frame):
        """Process incoming frame."""
        print(f"Received: {frame.data}")
        self.received.append(frame.data)

# Create instance
sink = MySink(name="printer_sink")
```

### Creating a Transform Element

A transform element both receives and produces frames. Implement both `pull()` and `new()`:

```python
from sgn.base import TransformElement, Frame

class Multiplier(TransformElement):
    def __init__(self, factor=2, **kwargs):
        super().__init__(
            source_pad_names=["output"],
            sink_pad_names=["input"],
            **kwargs
        )
        self.factor = factor
        self.current_data = None

    def pull(self, pad, frame):
        """Receive and store incoming frame."""
        self.current_data = frame.data

    def new(self, pad):
        """Generate new frame with transformed data."""
        return Frame(data=self.current_data * self.factor)

# Create instance
transform = Multiplier(factor=10, name="multiplier")
```

## Complete Example: Simple Pipeline

Here's a complete example combining all three element types:

```python
from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

# Define custom elements
class CounterSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.count = 0

    def new(self, pad):
        self.count += 1
        if self.count > 5:  # Stop after 5 frames
            return Frame(EOS=True)
        return Frame(data=self.count)

class Doubler(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(
            source_pad_names=["out"],
            sink_pad_names=["in"],
            **kwargs
        )
        self.current_frame = None

    def pull(self, pad, frame):
        self.current_frame = frame

    def new(self, pad):
        # Forward EOS if received
        if self.current_frame.EOS:
            return Frame(EOS=True)
        return Frame(data=self.current_frame.data * 2)

class PrinterSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        print(f"Result: {frame.data}")

# Build and run pipeline
pipeline = Pipeline()
source = CounterSource()
transform = Doubler()
sink = PrinterSink()

pipeline.insert(source, transform, sink)
pipeline.link({
    transform.snks["in"]: source.srcs["out"],
    sink.snks["in"]: transform.srcs["out"]
})

pipeline.run()
# Output:
# Result: 2
# Result: 4
# Result: 6
# Result: 8
# Result: 10
```

## Understanding Pads

### Source Pads vs Sink Pads

- **SourcePad**: Provides frames when called (outputs)
  - Created by `SourceElement` and `TransformElement`
  - Access via `element.source_pads` or `element.srcs["name"]`

- **SinkPad**: Receives frames when called (inputs)
  - Created by `TransformElement` and `SinkElement`
  - Access via `element.sink_pads` or `element.snks["name"]`

### Accessing Pads

Elements provide convenient shortcuts for accessing pads:

```python
from sgn.base import SourceElement, Frame

class MySource(SourceElement):
    def __init__(self, source_pad_names=None, **kwargs):
        if source_pad_names is None:
            source_pad_names = ["output"]
        super().__init__(source_pad_names=source_pad_names, **kwargs)
        self.counter = 0

    def new(self, pad):
        self.counter += 1
        return Frame(data=self.counter)

source = MySource(source_pad_names=["out1", "out2"], name="mysrc")

# Multiple ways to access pads:
pad1 = source.source_pads[0]                  # By index
pad2 = source.srcs["out1"]                    # By short name (recommended)
pad3 = source.source_pad_dict["mysrc:src:out1"]  # By full pad name
```

### Multiple Pads

Elements can have multiple input/output pads:

```python
from sgn.base import SourceElement, Frame

class MultiOutputSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(
            source_pad_names=["numbers", "letters"],
            **kwargs
        )
        self.counter = 0

    def new(self, pad):
        self.counter += 1

        # Different output based on which pad is calling
        if pad == self.srcs["numbers"]:
            return Frame(data=self.counter)
        else:  # letters pad
            return Frame(data=chr(64 + self.counter))  # A, B, C...

source = MultiOutputSource()
# source.srcs["numbers"] outputs: 1, 2, 3, ...
# source.srcs["letters"] outputs: 'A', 'B', 'C', ...
```

## Element Naming

Every element and pad has a unique name:

```python
from sgn.base import SourceElement, Frame

class MySource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["output"], **kwargs)
        self.counter = 0

    def new(self, pad):
        self.counter += 1
        return Frame(data=self.counter)

source = MySource(name="my_counter")
# Element name: "my_counter"
# Pad full name: "my_counter:src:output"

# If no name provided, a UUID is generated:
source2 = MySource()
# Element name: "a3f4b2c1d5e6..." (UUID)
```

!!! tip "Naming Best Practices"
    - Always provide meaningful names for debugging
    - Pad names are automatically prefixed with element name
    - Use `srcs` and `snks` dictionaries for cleaner code

## Advanced: Internal Pads and Element Lifecycle

Elements have an internal execution flow:

1. **Sink Pads**: Call `pull()` to receive data
2. **Internal Pad**: Call `internal()` for processing
3. **Source Pads**: Call `new()` to generate output

You can override the `internal()` method for custom logic:

```python
from sgn.base import TransformElement, Frame

class StatefulTransform(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(
            source_pad_names=["out"],
            sink_pad_names=["in"],
            **kwargs
        )
        self.buffer = []

    def pull(self, pad, frame):
        # Stage 1: Receive data
        self.buffer.append(frame.data)

    def internal(self):
        # Stage 2: Process between pull and new
        self.buffer = sorted(self.buffer)  # Sort accumulated data

    def new(self, pad):
        # Stage 3: Generate output
        return Frame(data=self.buffer.pop(0) if self.buffer else None)
```

## Frame Data Flow

Frames flow through the pipeline via pad connections:

```
[SourceElement]
      |
  SourcePad.output  ──┐
                      │ (linked)
                      ├─> SinkPad.input
                      │       |
              [TransformElement]
                      |
                 SourcePad.output  ──┐
                                     │ (linked)
                                     ├─> SinkPad.input
                                     │       |
                              [SinkElement]
```

!!! warning "Important: Pad Linking"
    - Sink pads must be linked to source pads before running
    - One sink pad can connect to only one source pad
    - Multiple sink pads can connect to the same source pad (fan-out)
    - Linking is done via `SinkPad.link()` or `Pipeline.link()`

## Related Tutorials

- [Hello World](../tutorials/hello_world.md) - Your first SGN pipeline
- [End of Stream](../tutorials/end_of_stream.md) - Handling pipeline termination
- [Multiple Pads](../tutorials/multiple_pads.md) - Working with multiple inputs/outputs
- [Transform Elements](../tutorials/transforms.md) - Data transformation patterns

## API Reference

::: sgn.base
    options:
      filters:
        - "!.*Like$"
