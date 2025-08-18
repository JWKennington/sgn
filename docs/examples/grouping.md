# Example: Element Grouping and Pad Selection

This example demonstrates the powerful grouping and pad selection features in SGN. These features allow you to:

- Group multiple elements together for convenient connection
- Select specific pads from elements to control data flow
- Combine grouping and selection for complex pipeline configurations

## Basic Element Grouping

Here's a simple example showing how to group multiple sources and connect them to multiple sinks:

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import group

# Create multiple sources with different data
src1 = IterSource(
    name="src1",
    source_pad_names=["H1"],
    iters={"src1:src:H1": [1, 2, 3]}
)

src2 = IterSource(
    name="src2",
    source_pad_names=["L1"],
    iters={"src2:src:L1": [10, 20, 30]}
)

# Create sinks for the data
sink1 = NullSink(name="sink1", sink_pad_names=["H1"])
sink2 = NullSink(name="sink2", sink_pad_names=["L1"])

# Group elements for convenient connection
sources = group(src1, src2)  # Contains H1 and L1 pads
sinks = group(sink1, sink2)  # Contains H1 and L1 pads

pipeline = Pipeline()
pipeline.connect(sources, sinks)  # Automatically connects H1->H1, L1->L1

pipeline.run()
```

## Pad Selection

Sometimes you only want to use specific pads from an element. Here's how to select only the pads you need:

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import select

# Create a source with multiple pads
multi_source = IterSource(
    name="multi_src",
    source_pad_names=["H1", "L1", "V1"],
    iters={
        "multi_src:src:H1": [1, 2, 3],
        "multi_src:src:L1": [10, 20, 30],
        "multi_src:src:V1": [100, 200, 300]  # We won't use this
    }
)

# Create a sink that only accepts H1 and L1
sink = NullSink(name="sink", sink_pad_names=["H1", "L1"])

# Select only the pads we need from the source
selected_pads = select(multi_source, "H1", "L1")  # Excludes V1

pipeline = Pipeline()
pipeline.connect(selected_pads, sink)  # Only H1 and L1 are connected
```

## Combined Grouping and Selection

For more complex scenarios, you can combine grouping and selection:

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import group, select

# Create sources with different pad configurations
dedicated_source = IterSource(
    name="dedicated",
    source_pad_names=["H1"],
    iters={"dedicated:src:H1": [1, 2, 3]}
)

multi_source = IterSource(
    name="multi",
    source_pad_names=["L1", "V1", "unused"],
    iters={
        "multi:src:L1": [10, 20, 30],
        "multi:src:V1": [100, 200, 300],
        "multi:src:unused": [1000, 2000, 3000]  # Not needed
    }
)

# Create sinks
h1_sink = NullSink(name="h1_sink", sink_pad_names=["H1"])
l1_sink = NullSink(name="l1_sink", sink_pad_names=["L1"])

# Group: include all pads from dedicated_source, only L1 from multi_source
sources = group(
    dedicated_source,           # All pads (H1)
    select(multi_source, "L1")  # Only L1 pad
)

sinks = group(h1_sink, l1_sink)

pipeline = Pipeline()
pipeline.connect(sources, sinks)  # H1->H1, L1->L1
```

## Explicit Linking with Groups

When automatic pad name matching isn't sufficient, you can still use explicit linking:

```python
from sgn import Pipeline, IterSource, NullSink
from sgn.groups import group

# Sources and sinks with different pad names
src1 = IterSource(name="src1", source_pad_names=["output_a"])
src2 = IterSource(name="src2", source_pad_names=["output_b"])

sink1 = NullSink(name="sink1", sink_pad_names=["input_x"])
sink2 = NullSink(name="sink2", sink_pad_names=["input_y"])

sources = group(src1, src2)
sinks = group(sink1, sink2)

pipeline = Pipeline()
pipeline.connect(sources, sinks, link_map={
    "input_x": "output_a",  # Map sink pad names to source pad names
    "input_y": "output_b"
})

pipeline.run()
```

## Linking Strategies

SGN automatically determines the appropriate linking strategy based on the number of source and sink pads:

### 1-to-1 Linking
```{.python notest}
# Perfect match: each source pad connects to one sink pad
sources = group(src_h1, src_l1)  # 2 pads: H1, L1
sinks = group(sink_h1, sink_l1)  # 2 pads: H1, L1
pipeline.connect(sources, sinks)  # H1->H1, L1->L1
```

### N-to-1 Linking (Fan-in)
```{.python notest}
# Multiple sources, single sink
sources = group(src1_h1, src2_h1, src3_h1)  # 3 H1 source pads
sink = NullSink(name="sink", sink_pad_names=["H1"])  # 1 H1 sink pad
pipeline.connect(sources, sink)  # All H1 sources -> H1 sink
```

### 1-to-N Linking (Fan-out)
```{.python notest}
# Single source, multiple sinks
src = IterSource(name="src", source_pad_names=["H1"])  # 1 H1 source pad
sinks = group(sink1_h1, sink2_h1, sink3_h1)  # 3 H1 sink pads
pipeline.connect(src, sinks)  # H1 source -> all H1 sinks
```

This grouping system makes it easy to build complex pipelines while maintaining clear, readable code.
