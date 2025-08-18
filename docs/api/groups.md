# Groups Module

The `sgn.groups` module provides powerful utilities for grouping elements and selecting specific pads in SGN pipelines. This module enables more intuitive and flexible pipeline construction by allowing you to work with collections of elements and specific pad selections.

## Key Concepts

### Element Grouping
Element grouping allows you to treat multiple elements as a single unit when connecting pipelines. This is particularly useful when you have multiple sources that need to connect to multiple sinks, or when you want to organize elements logically.

### Pad Selection
Pad selection enables you to specify exactly which pads from an element should be used in pipeline operations. This provides fine-grained control over data flow and helps prevent unintended connections.

## Classes and Functions

::: sgn.groups.PadSelection
    options:
      show_source: true

::: sgn.groups.ElementGroup
    options:
      show_source: true

::: sgn.groups.select
    options:
      show_source: true

::: sgn.groups.group
    options:
      show_source: true

## Usage Examples

### Basic Element Grouping

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import group

# Create multiple sources
src1 = IterSource(name="src1", source_pad_names=["H1"])
src2 = IterSource(name="src2", source_pad_names=["L1"])

# Create multiple sinks
sink1 = NullSink(name="sink1", sink_pad_names=["H1"])
sink2 = NullSink(name="sink2", sink_pad_names=["L1"])

# Group elements for convenient connection
sources = group(src1, src2)
sinks = group(sink1, sink2)

pipeline = Pipeline()
pipeline.connect(sources, sinks)  # Connects H1->H1, L1->L1 automatically
```

### Pad Selection

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import select

# Create a source with multiple pads
src = IterSource(name="src", source_pad_names=["H1", "L1", "V1"])

# Create a sink that only needs specific data
sink = NullSink(name="sink", sink_pad_names=["H1", "L1"])

# Select only the pads we need from the source
selected_pads = select(src, "H1", "L1")  # Exclude V1

pipeline = Pipeline()
pipeline.connect(selected_pads, sink)  # Only H1 and L1 are connected
```

### Combined Grouping and Selection

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import group, select

# Create sources with different pad configurations
src1 = IterSource(name="src1", source_pad_names=["H1"])
src2 = IterSource(name="src2", source_pad_names=["L1", "V1", "extra"])

# Create a sink that expects specific pads
sink = NullSink(name="sink", sink_pad_names=["H1", "L1"])

# Group elements, selecting only the needed pads from src2
sources = group(src1, select(src2, "L1"))  # src1 all pads, src2 only L1

pipeline = Pipeline()
pipeline.connect(sources, sink)  # H1->H1, L1->L1
```

### Explicit Mapping with Groups

You can still use explicit mapping when working with groups for cases where automatic matching isn't sufficient:

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import group

# Create sources and sinks with different pad names
src1 = IterSource(name="src1", source_pad_names=["out1"])
src2 = IterSource(name="src2", source_pad_names=["out2"])

sink1 = NullSink(name="sink1", sink_pad_names=["in1"])
sink2 = NullSink(name="sink2", sink_pad_names=["in2"])

sources = group(src1, src2)
sinks = group(sink1, sink2)

pipeline = Pipeline()
pipeline.connect(sources, sinks, link_map={
    "in1": "out1",  # Map sink pad names to source pad names
    "in2": "out2"
})
```

## Linking Strategies

When using `pipeline.connect()` with groups, SGN employs intelligent linking strategies:

### 1-to-1 Matching
When source and sink groups have the same number of pads with matching names:
```{.python notest}
# H1->H1, L1->L1 (perfect match)
sources = group(src_h1, src_l1)  # H1, L1 pads
sinks = group(sink_h1, sink_l1)  # H1, L1 pads
pipeline.connect(sources, sinks)
```

### N-to-1 Linking
Multiple source pads connecting to a single sink pad:
```{.python notest}
# Two sources -> single H1 sink
sources = group(src1, src2)  # Two source pads
sink = NullSink(name="sink", sink_pad_names=["H1"])  # One H1 pad
pipeline.connect(sources, sink)
```

### 1-to-N Linking
Single source pad connecting to multiple sink pads:
```{.python notest}
# Single H1 source -> two sinks
src = IterSource(name="src", source_pad_names=["H1"])  # One source pad
sinks = group(sink1, sink2)  # Two sink pads
pipeline.connect(src, sinks)
```
