# Pipeline Connection Basics

This tutorial covers the two main ways to connect elements in an SGN pipeline: the modern `connect()` method and the traditional `insert()` method with explicit `link_map`.

## Overview

SGN provides two approaches for building pipelines:

1. **`pipeline.connect()`** - Modern, intuitive API with automatic pad matching
2. **`pipeline.insert()` with `link_map`** - Traditional API with explicit connections

In most cases, you'll want to use `connect()` as it's simpler and more readable. However, understanding both methods helps you work with existing code and handle complex scenarios.

## The Modern Way: pipeline.connect()

The `connect()` method automatically matches pads by name and inserts elements into the pipeline for you.

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class MySource(SourceElement):
    def new(self, pad):
        return Frame(data="Hello from connect()!")

class MySink(SinkElement):
    def pull(self, pad, frame):
        print(frame.data)
        if frame.EOS:
            self.mark_eos(pad)

# Create elements with matching pad names
source = MySource(source_pad_names=("data",))
sink = MySink(sink_pad_names=("data",))

# Create pipeline and connect
pipeline = Pipeline()
pipeline.connect(source, sink)  # Automatically links "data" -> "data"

pipeline.run()
```

**Key benefits:**
- Automatically inserts elements into the pipeline
- Matches pads by name (no manual mapping needed)
- Cleaner, more readable code
- Less error-prone

## The Traditional Way: pipeline.insert() with link_map

The `insert()` method requires you to explicitly specify how pads connect using a `link_map` dictionary.

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class MySource(SourceElement):
    def new(self, pad):
        return Frame(data="Hello from insert()!")

class MySink(SinkElement):
    def pull(self, pad, frame):
        print(frame.data)
        if frame.EOS:
            self.mark_eos(pad)

# Create elements (pad names don't need to match)
source = MySource(source_pad_names=("output",))
sink = MySink(sink_pad_names=("input",))

# Create pipeline and insert with explicit link_map
pipeline = Pipeline()
pipeline.insert(
    source,
    sink,
    link_map={
        sink.snks["input"]: source.srcs["output"]  # sink <- source
    }
)

pipeline.run()
```

**When to use insert():**
- Working with legacy code that uses this pattern
- Pad names don't match and you want to keep them that way
- You need very explicit control over connections
- Building reusable element libraries where pad names vary

## Understanding link_map

The `link_map` is a dictionary where:
- **Keys** = sink pads (data destination)
- **Values** = source pads (data origin)

```{.python notest}
link_map = {
    sink_pad: source_pad  # Data flows: source_pad -> sink_pad
}
```

You can use either pad objects or pad name strings:

```{.python notest}
# Using pad objects (recommended)
link_map = {sink.snks["x"]: source.srcs["a"]}

# Using pad name strings
link_map = {"sink_name:snk:x": "source_name:src:a"}
```

## Side-by-Side Comparison

Here's the same pipeline built both ways:

### Using connect()
```{.python notest}
pipeline = Pipeline()
pipeline.connect(source, transform)
pipeline.connect(transform, sink)
```

### Using insert()
```{.python notest}
pipeline = Pipeline()
pipeline.insert(
    source,
    transform,
    sink,
    link_map={
        transform.snks["in"]: source.srcs["out"],
        sink.snks["in"]: transform.srcs["out"]
    }
)
```

## Recommendation

**Use `pipeline.connect()` for new code.** It's simpler, cleaner, and handles most use cases automatically. You'll learn about its intelligent connection strategies in the next tutorial.

**Use `pipeline.insert()` with `link_map` only when:**
- Maintaining legacy code
- Pad names intentionally differ and you want explicit control
- Automatic strategies are ambiguous (rare - usually solvable with grouping)

## Next Steps

Now that you understand the basics, learn how `connect()` intelligently determines connection strategies in the [Automatic Connection Strategies](pipeline_connection_strategies.md) tutorial.
