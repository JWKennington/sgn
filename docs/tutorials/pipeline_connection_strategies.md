# Automatic Connection Strategies

The `pipeline.connect()` method is intelligent - it automatically determines how to link pads based on their names and counts. This tutorial explains the four connection strategies and when each is used.

## The Four Strategies

When you call `pipeline.connect(source, sink)`, SGN analyzes the pads and chooses one of these strategies:

1. **1-to-1 Matching** - Same pad names on both sides
2. **N-to-1 Linking** - Many source pads → one sink pad
3. **1-to-N Linking** - One source pad → many sink pads
4. **Explicit link_map** - You specify the connections

Let's explore each with examples.

## Strategy 1: 1-to-1 Matching (Same Pad Names)

**When:** Source and sink have the same pad names

**What happens:** Pads are automatically matched by name

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class DataSource(SourceElement):
    def new(self, pad):
        return Frame(data=f"Data from {pad.name}")

class DataSink(SinkElement):
    def pull(self, pad, frame):
        print(f"{pad.name}: {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Both have matching pad names: "sensor1" and "sensor2"
source = DataSource(source_pad_names=("sensor1", "sensor2"))
sink = DataSink(sink_pad_names=("sensor1", "sensor2"))

pipeline = Pipeline()
pipeline.connect(source, sink)  # Auto-matches: sensor1->sensor1, sensor2->sensor2

pipeline.run()
```

**Output:**
```text
sensor1: Data from sensor1
sensor2: Data from sensor2
```

**This is the most common pattern** - design your pad names to match and connections happen automatically!

## Strategy 2: N-to-1 Linking (Fan-In)

**When:** Multiple source pads, exactly one sink pad

**What happens:** All source pads connect to the single sink pad

**Use case:** Combining multiple data streams into one processor

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, TransformElement, Frame
from sgn.apps import Pipeline

class MultiSource(SourceElement):
    def new(self, pad):
        return Frame(data=f"Stream {pad.name}")

class Combiner(SinkElement):
    def __post_init__(self):
        super().__post_init__()
        self.buffer = []

    def pull(self, pad, frame):
        self.buffer.append(frame.data)
        if frame.EOS:
            self.mark_eos(pad)

    def internal(self):
        if self.buffer:
            print(f"Combined: {', '.join(self.buffer)}")
            self.buffer = []

# Multiple source pads -> single sink pad
source = MultiSource(source_pad_names=("A", "B", "C"))
combiner = Combiner(sink_pad_names=("input",))  # Only one sink pad

pipeline = Pipeline()
pipeline.connect(source, combiner)  # All sources -> "input"

pipeline.run()
```

**Output:**
```text
Combined: Stream A, Stream B, Stream C
```

All three source pads automatically connect to the single sink pad.

## Strategy 3: 1-to-N Linking (Fan-Out)

**When:** One source pad, multiple sink pads

**What happens:** The source pad connects to all sink pads

**Use case:** Broadcasting data to multiple consumers

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class Broadcaster(SourceElement):
    def __post_init__(self):
        super().__post_init__()
        self.count = 0

    def new(self, pad):
        self.count += 1
        return Frame(data=f"Broadcast {self.count}", EOS=self.count > 3)

class Receiver(SinkElement):
    def pull(self, pad, frame):
        print(f"{self.name}.{pad.name}: {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# One source pad -> multiple sink pads
broadcaster = Broadcaster(source_pad_names=("output",))  # Only one source pad
receiver = Receiver(name="receiver", sink_pad_names=("in1", "in2", "in3"))

pipeline = Pipeline()
pipeline.connect(broadcaster, receiver)  # "output" -> all sink pads

pipeline.run()
```

**Output:**
```text
receiver.in1: Broadcast 1
receiver.in2: Broadcast 1
receiver.in3: Broadcast 1
receiver.in1: Broadcast 2
receiver.in2: Broadcast 2
receiver.in3: Broadcast 2
...
```

The single source pad broadcasts to all three sink pads.

## Strategy 4: Explicit link_map

**When:** Automatic strategies are ambiguous (multiple source pads AND multiple sink pads with different names)

**What happens:** You must provide an explicit `link_map`

**Use case:** Complex routing where automatic strategies can't determine intent

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class MultiSource(SourceElement):
    def new(self, pad):
        return Frame(data=f"From {pad.name}")

class MultiSink(SinkElement):
    def pull(self, pad, frame):
        print(f"To {pad.name}: {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Multiple pads on both sides, different names
source = MultiSource(source_pad_names=("out_A", "out_B"))
sink = MultiSink(sink_pad_names=("in_X", "in_Y"))

pipeline = Pipeline()

# This would raise an error - ambiguous!
# pipeline.connect(source, sink)

# Explicit mapping required
pipeline.connect(
    source,
    sink,
    link_map={
        "in_X": "out_A",  # Route out_A -> in_X
        "in_Y": "out_B",  # Route out_B -> in_Y
    }
)

pipeline.run()
```

**Output:**
```text
To in_X: From out_A
To in_Y: From out_B
```

## Strategy Selection Flow

Here's how SGN decides which strategy to use:

```
┌─────────────────────────────────┐
│  pipeline.connect(source, sink) │
└────────────┬────────────────────┘
             │
             ▼
     ┌───────────────┐
     │ link_map      │  YES
     │ provided?     ├──────> Use explicit mapping
     └───────┬───────┘
             │ NO
             ▼
     ┌───────────────┐
     │ Pad names     │  YES
     │ match?        ├──────> 1-to-1 matching
     └───────┬───────┘
             │ NO
             ▼
     ┌───────────────┐
     │ Single sink   │  YES
     │ pad?          ├──────> N-to-1 (Fan-in)
     └───────┬───────┘
             │ NO
             ▼
     ┌───────────────┐
     │ Single source │  YES
     │ pad?          ├──────> 1-to-N (Fan-out)
     └───────┬───────┘
             │ NO
             ▼
     ┌───────────────┐
     │ Error:        │
     │ Ambiguous!    │
     │ Need link_map │
     └───────────────┘
```

## Best Practices

1. **Design pad names to match** - Enables automatic 1-to-1 linking
2. **Use single pads for fan-in/out** - Makes intent clear
3. **Provide link_map for complex routing** - Don't fight the system
4. **Group elements strategically** - More on this in the next tutorial!

## Common Patterns

### Pipeline Chain
```{.python notest}
# All use 1-to-1 matching with pad name "data"
pipeline.connect(source, transform1)
pipeline.connect(transform1, transform2)
pipeline.connect(transform2, sink)
```

### Multi-Source Aggregation
```{.python notest}
# N-to-1: Many sources -> one sink
pipeline.connect(group(source1, source2, source3), aggregator)
```

### Broadcast to Multiple Sinks
```{.python notest}
# 1-to-N: One source -> many sinks
pipeline.connect(broadcaster, group(sink1, sink2, sink3))
```

## Next Steps

Learn how to use [Element Grouping](pipeline_grouping.md) to manage multiple elements and create cleaner connection patterns.
