# Element Grouping

As pipelines grow more complex with many elements, managing connections becomes challenging. SGN's `group()` function lets you organize multiple elements and connect them as a unit, making your code cleaner and more maintainable.

## Why Use Grouping?

Without grouping, connecting multiple sources to a sink looks like this:

```{.python notest}
# Without grouping - repetitive
pipeline.connect(source1, sink)
pipeline.connect(source2, sink)
pipeline.connect(source3, sink)
pipeline.connect(source4, sink)
```

With grouping:

```{.python notest}
# With grouping - clean and clear
from sgn.groups import group

sources = group(source1, source2, source3, source4)
pipeline.connect(sources, sink)
```

## Basic Grouping

The `group()` function combines multiple elements into an `ElementGroup`:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group

class Sensor(SourceElement):
    def __post_init__(self):
        super().__post_init__()
        self.count = 0

    def new(self, pad):
        self.count += 1
        return Frame(data=f"{self.name}: reading {self.count}", EOS=self.count > 3)

class Logger(SinkElement):
    def pull(self, pad, frame):
        print(frame.data)
        if frame.EOS:
            self.mark_eos(pad)

# Create multiple sensors
sensor1 = Sensor(name="temp_sensor", source_pad_names=("data",))
sensor2 = Sensor(name="pressure_sensor", source_pad_names=("data",))
sensor3 = Sensor(name="humidity_sensor", source_pad_names=("data",))

# Create a logger
logger = Logger(name="logger", sink_pad_names=("data",))

# Group all sensors
sensors = group(sensor1, sensor2, sensor3)

# Connect the entire group at once
pipeline = Pipeline()
pipeline.connect(sensors, logger)  # All sensors -> logger (N-to-1)

pipeline.run()
```

**Output:**
```text
temp_sensor: reading 1
pressure_sensor: reading 1
humidity_sensor: reading 1
temp_sensor: reading 2
pressure_sensor: reading 2
humidity_sensor: reading 2
...
```

All three sensors automatically connect to the logger using the N-to-1 strategy.

## Grouping with Multiple Sink Pads

Groups work with any connection strategy. Here's 1-to-N with a group on the sink side:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group

class Broadcaster(SourceElement):
    def __post_init__(self):
        super().__post_init__()
        self.count = 0

    def new(self, pad):
        self.count += 1
        return Frame(data=f"Broadcast message {self.count}", EOS=self.count > 3)

class Display(SinkElement):
    def pull(self, pad, frame):
        print(f"[{self.name}] {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Create one broadcaster
broadcaster = Broadcaster(name="broadcaster", source_pad_names=("output",))

# Create multiple displays
display1 = Display(name="monitor1", sink_pad_names=("input",))
display2 = Display(name="monitor2", sink_pad_names=("input",))
display3 = Display(name="monitor3", sink_pad_names=("input",))

# Group all displays
displays = group(display1, display2, display3)

# Connect broadcaster to all displays (1-to-N)
pipeline = Pipeline()
pipeline.connect(broadcaster, displays)

pipeline.run()
```

**Output:**
```text
[monitor1] Broadcast message 1
[monitor2] Broadcast message 1
[monitor3] Broadcast message 1
[monitor1] Broadcast message 2
[monitor2] Broadcast message 2
[monitor3] Broadcast message 2
...
```

## Selecting Subsets from Groups

The `.select()` method extracts specific elements from a group by name:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group

class DataSource(SourceElement):
    def new(self, pad):
        return Frame(data=f"Data from {self.name}")

class DataSink(SinkElement):
    def pull(self, pad, frame):
        print(f"{self.name}: {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Create sources
source_A = DataSource(name="source_A", source_pad_names=("data",))
source_B = DataSource(name="source_B", source_pad_names=("data",))
source_C = DataSource(name="source_C", source_pad_names=("data",))

# Create sinks
sink_X = DataSink(name="sink_X", sink_pad_names=("data",))
sink_Y = DataSink(name="sink_Y", sink_pad_names=("data",))

# Group all sources
all_sources = group(source_A, source_B, source_C)

# Select only A and B for sink_X
pipeline = Pipeline()
pipeline.connect(all_sources.select("source_A", "source_B"), sink_X)

# Select only C for sink_Y
pipeline.connect(all_sources.select("source_C"), sink_Y)

pipeline.run()
```

**Output:**
```text
sink_X: Data from source_A
sink_X: Data from source_B
sink_Y: Data from source_C
```

## Nesting Groups

You can nest groups within groups for hierarchical organization:

```{.python notest}
from sgn.groups import group

# Create groups by type
temperature_sensors = group(temp1, temp2, temp3)
pressure_sensors = group(pressure1, pressure2)

# Create a super-group
all_sensors = group(temperature_sensors, pressure_sensors)

# Connect the entire hierarchy
pipeline.connect(all_sensors, aggregator)
```

When you group existing groups, they are flattened into a single group containing all elements.

## Practical Example: Multi-Source Data Pipeline

Here's a realistic example combining everything:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group

class Sensor(SourceElement):
    def __post_init__(self):
        super().__post_init__()
        self.count = 0

    def new(self, pad):
        self.count += 1
        return Frame(
            data={"sensor": self.name, "value": self.count * 10},
            EOS=self.count > 5
        )

class Validator(TransformElement):
    def __post_init__(self):
        super().__post_init__()
        self.last_frame = None

    def pull(self, pad, frame):
        self.last_frame = frame

    def new(self, pad):
        if self.last_frame and self.last_frame.data:
            # Only pass through if value is valid
            value = self.last_frame.data.get("value", 0)
            if value <= 50:  # Validation rule
                return self.last_frame
        return Frame(data=None, EOS=self.last_frame.EOS if self.last_frame else False)

class Database(SinkElement):
    def pull(self, pad, frame):
        if frame.data:
            print(f"Saving to DB: {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Create 4 sensors
sensors = group(
    Sensor(name="sensor_1", source_pad_names=("data",)),
    Sensor(name="sensor_2", source_pad_names=("data",)),
    Sensor(name="sensor_3", source_pad_names=("data",)),
    Sensor(name="sensor_4", source_pad_names=("data",))
)

# Create validators for each sensor
validators = group(
    Validator(name="validator_1", sink_pad_names=("data",), source_pad_names=("data",)),
    Validator(name="validator_2", sink_pad_names=("data",), source_pad_names=("data",)),
    Validator(name="validator_3", sink_pad_names=("data",), source_pad_names=("data",)),
    Validator(name="validator_4", sink_pad_names=("data",), source_pad_names=("data",))
)

# Create database sink
db = Database(name="database", sink_pad_names=("data",))

# Build pipeline with groups
pipeline = Pipeline()
pipeline.connect(sensors.select("sensor_1"), validators.select("validator_1"))
pipeline.connect(sensors.select("sensor_2"), validators.select("validator_2"))
pipeline.connect(sensors.select("sensor_3"), validators.select("validator_3"))
pipeline.connect(sensors.select("sensor_4"), validators.select("validator_4"))
pipeline.connect(validators, db)

pipeline.run()
```

This pipeline processes data from 4 sensors, validates each stream independently, then aggregates valid data into a database.

## Best Practices

1. **Group related elements** - Sensors together, processors together, outputs together
2. **Use descriptive group variables** - `sensors`, `validators`, `loggers` instead of `group1`, `group2`
3. **Combine with .select()** - Route subsets to different destinations
4. **Flatten hierarchies** - Groups automatically flatten when nested
5. **Think in layers** - Sources → Transforms → Sinks

## Common Patterns

### Multi-Source Aggregation
```{.python notest}
sources = group(source1, source2, source3)
pipeline.connect(sources, aggregator)
```

### Broadcast to Multiple Consumers
```{.python notest}
consumers = group(logger, database, cache)
pipeline.connect(producer, consumers)
```

### Parallel Processing Lanes
```{.python notest}
lane1 = group(source1, transform1, sink1)
lane2 = group(source2, transform2, sink2)
# Connect each lane separately
```

## Next Steps

Learn how to use [Pad Selection](pipeline_selection.md) for even finer control over which specific pads connect, especially useful when elements have multiple pads with different purposes.
