# Pipeline - Orchestrating SGN Task Graphs

The `Pipeline` class is the central orchestrator for SGN applications. It manages the directed acyclic graph (DAG) of elements, handles pad connections, and executes your data processing pipeline asynchronously.

## Overview

A `Pipeline` coordinates three key responsibilities:

1. **Element Management** - Register and track elements in your graph
2. **Connection Management** - Link pads between elements to define data flow
3. **Execution** - Run the async event loop to process frames through the graph

## Quick Start: Your First Pipeline

Here's a minimal working pipeline:

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class HelloSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.count = 0

    def new(self, pad):
        self.count += 1
        if self.count > 3:
            return Frame(EOS=True)
        return Frame(data=f"Hello {self.count}")

class PrinterSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        print(frame.data)

# Build and run pipeline
p = Pipeline()
source = HelloSource()
sink = PrinterSink()

p.insert(source, sink)
p.link({sink.snks["in"]: source.srcs["out"]})
p.run()
# Output:
# Hello 1
# Hello 2
# Hello 3
```

## Connecting Elements: Two Approaches

### Approach 1: insert() + link() (Explicit)

The traditional approach gives you fine-grained control over connections:

```python
from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

class Counter(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["numbers"], **kwargs)
        self.n = 0

    def new(self, pad):
        self.n += 1
        if self.n > 5:
            return Frame(EOS=True)
        return Frame(data=self.n)

class Doubler(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(
            source_pad_names=["doubled"],
            sink_pad_names=["input"],
            **kwargs
        )
        self.value = None

    def pull(self, pad, frame):
        self.value = frame

    def new(self, pad):
        if self.value.EOS:
            return Frame(EOS=True)
        return Frame(data=self.value.data * 2)

class Printer(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["data"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        print(f"Result: {frame.data}")

# Explicit connection
p = Pipeline()
source = Counter()
transform = Doubler()
sink = Printer()

# Insert all elements
p.insert(source, transform, sink)

# Link pads explicitly (data flows right to left in dict)
p.link({
    transform.snks["input"]: source.srcs["numbers"],  # source -> transform
    sink.snks["data"]: transform.srcs["doubled"]       # transform -> sink
})

p.run()
# Output:
# Result: 2
# Result: 4
# Result: 6
# Result: 8
# Result: 10
```

!!! tip "link() Dictionary Format"
    In `link_map`, data flows **from value to key**:
    ```python
    {sink_pad: source_pad}  # Data flows from source_pad -> sink_pad
    ```

### Approach 2: connect() (Automatic Matching)

The modern `connect()` method automatically matches pads by name:

```python
from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

class NumberSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["data"], **kwargs)
        self.n = 0

    def new(self, pad):
        self.n += 1
        if self.n > 3:
            return Frame(EOS=True)
        return Frame(data=self.n)

class Squarer(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(
            source_pad_names=["data"],
            sink_pad_names=["data"],
            **kwargs
        )
        self.input = None

    def pull(self, pad, frame):
        self.input = frame

    def new(self, pad):
        if self.input.EOS:
            return Frame(EOS=True)
        return Frame(data=self.input.data ** 2)

class ResultSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["data"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        print(f"Squared: {frame.data}")

# Automatic connection (all pads named "data")
p = Pipeline()
source = NumberSource()
transform = Squarer()
sink = ResultSink()

p.connect(source, transform)  # Matches "data" pads
p.connect(transform, sink)    # Matches "data" pads

p.run()
# Output:
# Squared: 1
# Squared: 4
# Squared: 9
```

!!! success "When to Use connect()"
    Use `connect()` when:
    - Pad names match between elements
    - You want cleaner, more readable code
    - Building simple linear pipelines

## Advanced Connection Patterns

### Multiple Source Pads

Elements can have multiple output pads for different data streams:

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class DualSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["evens", "odds"], **kwargs)
        self.n = 0

    def new(self, pad):
        self.n += 1
        if self.n > 6:
            return Frame(EOS=True)

        # Different output based on pad
        if pad == self.srcs["evens"]:
            return Frame(data=self.n if self.n % 2 == 0 else None)
        else:  # odds pad
            return Frame(data=self.n if self.n % 2 == 1 else None)

class NumberPrinter(SinkElement):
    def __init__(self, label, **kwargs):
        super().__init__(sink_pad_names=["numbers"], **kwargs)
        self.label = label

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        if frame.data is not None:
            print(f"{self.label}: {frame.data}")

p = Pipeline()
source = DualSource()
even_sink = NumberPrinter("Even")
odd_sink = NumberPrinter("Odd")

p.insert(source, even_sink, odd_sink)
p.link({
    even_sink.snks["numbers"]: source.srcs["evens"],
    odd_sink.snks["numbers"]: source.srcs["odds"]
})

p.run()
# Output:
# Odd: 1
# Even: 2
# Odd: 3
# Even: 4
# Odd: 5
# Even: 6
```

### Fan-Out: One Source, Multiple Sinks

Multiple sinks can connect to the same source pad:

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class DataSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["data"], **kwargs)
        self.n = 0

    def new(self, pad):
        self.n += 1
        if self.n > 3:
            return Frame(EOS=True)
        return Frame(data=self.n * 10)

class Sink1(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        print(f"Sink1 received: {frame.data}")

class Sink2(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        print(f"Sink2 received: {frame.data}")

p = Pipeline()
source = DataSource()
sink1 = Sink1()
sink2 = Sink2()

p.insert(source, sink1, sink2)
p.link({
    sink1.snks["in"]: source.srcs["data"],  # Fan-out: same source
    sink2.snks["in"]: source.srcs["data"]   # to multiple sinks
})

p.run()
# Output:
# Sink1 received: 10
# Sink2 received: 10
# Sink1 received: 20
# Sink2 received: 20
# Sink1 received: 30
# Sink2 received: 30
```

## Pipeline Lifecycle

### 1. Build Phase

```python
from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

class SimpleSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
    def new(self, pad):
        return Frame(data=1)

class SimpleTransform(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], sink_pad_names=["in"], **kwargs)
        self.data = None
    def pull(self, pad, frame):
        self.data = frame
    def new(self, pad):
        return self.data

class SimpleSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)
    def pull(self, pad, frame):
        pass

p = Pipeline()

# Add elements
source = SimpleSource()
transform = SimpleTransform()
sink = SimpleSink()
p.insert(source, transform, sink)

# Define connections
p.link({transform.snks["in"]: source.srcs["out"]})
p.link({sink.snks["in"]: transform.srcs["out"]})
```

### 2. Execution Phase

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class QuickSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.n = 0
    def new(self, pad):
        self.n += 1
        if self.n > 2:
            return Frame(EOS=True)
        return Frame(data=self.n)

class QuickSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

p = Pipeline()
p.insert(QuickSource(), QuickSink())
p.link({p.elements[1].snks["in"]: p.elements[0].srcs["out"]})

# Run the pipeline (blocking until all sinks reach EOS)
p.run()
```

### 3. Accessing Results

```python
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class DataSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.n = 0
    def new(self, pad):
        self.n += 1
        if self.n > 3:
            return Frame(EOS=True)
        return Frame(data=self.n * 10)

class CollectorSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)
        self.collected_data = []
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
            return
        self.collected_data.append(frame.data)

p = Pipeline()
source = DataSource()
sink = CollectorSink(name="sink_name")
p.insert(source, sink)
p.link({sink.snks["in"]: source.srcs["out"]})
p.run()

# Access collected data after run()
sink = p["sink_name"]  # Get element by name
results = sink.collected_data
print(f"Collected: {results}")  # [10, 20, 30]
```

## Accessing Pipeline Elements

Elements and pads are accessible by name:

```python
from sgn.base import SourceElement, Frame
from sgn.apps import Pipeline

class MySource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
    def new(self, pad):
        return Frame(data=1)

p = Pipeline()
source = MySource(name="my_source")
p.insert(source)

# Access element by name
elem = p["my_source"]

# Access pad by full name
pad = p["my_source:src:out"]
```

## Common Patterns

### Pattern 1: Linear Pipeline

```python
from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

class Source(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.n = 0
    def new(self, pad):
        self.n += 1
        if self.n > 2:
            return Frame(EOS=True)
        return Frame(data=self.n)

class Transform(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], sink_pad_names=["in"], **kwargs)
        self.val = None
    def pull(self, pad, frame):
        self.val = frame
    def new(self, pad):
        if self.val.EOS:
            return Frame(EOS=True)
        return Frame(data=self.val.data * 2)

class Sink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

p = Pipeline()
source = Source()
transform1 = Transform()
transform2 = Transform()
sink = Sink()

p.insert(source, transform1, transform2, sink)
p.link({
    transform1.snks["in"]: source.srcs["out"],
    transform2.snks["in"]: transform1.srcs["out"],
    sink.snks["in"]: transform2.srcs["out"]
})
p.run()
```

### Pattern 2: Using connect() for Linear Pipelines

```python
from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

class DataSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["data"], **kwargs)
        self.n = 0
    def new(self, pad):
        self.n += 1
        if self.n > 2:
            return Frame(EOS=True)
        return Frame(data=self.n)

class DataTransform(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["data"], sink_pad_names=["data"], **kwargs)
        self.val = None
    def pull(self, pad, frame):
        self.val = frame
    def new(self, pad):
        if self.val.EOS:
            return Frame(EOS=True)
        return Frame(data=self.val.data + 1)

class DataSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["data"], **kwargs)
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

p = Pipeline()
source = DataSource()
transform1 = DataTransform()
transform2 = DataTransform()
sink = DataSink()

p.connect(source, transform1)
p.connect(transform1, transform2)
p.connect(transform2, sink)
p.run()
```

### Pattern 3: Insert with Inline link_map

```python
from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

class NumSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
        self.n = 0
    def new(self, pad):
        self.n += 1
        if self.n > 2:
            return Frame(EOS=True)
        return Frame(data=self.n)

class NumTransform(TransformElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], sink_pad_names=["in"], **kwargs)
        self.val = None
    def pull(self, pad, frame):
        self.val = frame
    def new(self, pad):
        if self.val.EOS:
            return Frame(EOS=True)
        return Frame(data=self.val.data * 3)

class NumSink(SinkElement):
    def __init__(self, **kwargs):
        super().__init__(sink_pad_names=["in"], **kwargs)
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

p = Pipeline()
source = NumSource()
transform = NumTransform()
sink = NumSink()

p.insert(
    source, transform, sink,
    link_map={
        transform.snks["in"]: source.srcs["out"],
        sink.snks["in"]: transform.srcs["out"]
    }
)
p.run()
```

## Error Handling

The pipeline validates connections and will raise errors for:

- **Duplicate element names**
- **Invalid pad types** (e.g., connecting two source pads)
- **Missing elements** (accessing non-existent elements)
- **Circular dependencies** (detected during execution)

```python
from sgn.base import SourceElement, Frame
from sgn.apps import Pipeline

class TestSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(source_pad_names=["out"], **kwargs)
    def new(self, pad):
        return Frame(data=1)

# This will raise an error: duplicate names
p = Pipeline()
p.insert(TestSource(name="source"))
try:
    p.insert(TestSource(name="source"))  # AssertionError!
except AssertionError as e:
    print(f"Caught error: {e}")
```

## Visualization

Generate a visual graph of your pipeline:

```{.python notest}
from sgn.apps import Pipeline
from sgn.visualize import visualize

p = Pipeline()
p.insert(source, transform, sink)
p.link({...})

# Generate graphviz diagram
visualize(p, filename="my_pipeline")
# Creates my_pipeline.pdf
```

!!! note "Requires Graphviz"
    Install graphviz: `pip install sgn[visualize]`

## Related Tutorials

- [Connection Basics](../tutorials/pipeline_connection_basics.md) - Detailed guide to `connect()` vs `insert()`
- [Connection Strategies](../tutorials/pipeline_connection_strategies.md) - Automatic pad matching patterns
- [Element Grouping](../tutorials/pipeline_grouping.md) - Working with element groups
- [Pipeline Visualization](../tutorials/pipeline_visualization.md) - Creating pipeline diagrams

## API Reference

::: sgn.apps
