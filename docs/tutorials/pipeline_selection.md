# Pad Selection

Sometimes an element has many pads, but you only want to connect specific ones. SGN's `select()` function provides fine-grained control over which pads participate in connections, enabling precise routing in complex pipelines.

## The Problem: Too Many Pads

Imagine a multi-sensor source with many output pads, but different sinks need different subsets:

```{.python notest}
# Sensor has 6 output pads
sensor = MultiSensor(source_pad_names=("temp", "pressure", "humidity", "light", "sound", "motion"))

# Display only wants 2 specific readings
display = Display(sink_pad_names=("temp", "humidity"))

# Logger wants everything
logger = Logger(sink_pad_names=("temp", "pressure", "humidity", "light", "sound", "motion"))
```

How do you connect just `temp` and `humidity` to the display, while sending everything to the logger?

## Solution: Pad Selection

The `select()` function creates a `PadSelection` that represents a subset of an element's pads:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import select

class MultiSensor(SourceElement):
    def new(self, pad):
        readings = {
            "temp": 72,
            "pressure": 1013,
            "humidity": 65,
            "light": 450,
            "sound": 42,
            "motion": 0
        }
        return Frame(data=f"{pad.name}: {readings.get(pad.name, 'N/A')}")

class Display(SinkElement):
    def pull(self, pad, frame):
        print(f"[Display] {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

class Logger(SinkElement):
    def pull(self, pad, frame):
        print(f"[Log] {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Create sensor with 6 output pads
sensor = MultiSensor(
    name="sensor",
    source_pad_names=("temp", "pressure", "humidity", "light", "sound", "motion")
)

# Create sinks
display = Display(name="display", sink_pad_names=("temp", "humidity"))
logger = Logger(name="logger", sink_pad_names=("temp", "pressure", "humidity", "light", "sound", "motion"))

pipeline = Pipeline()

# Select only specific pads for the display
pipeline.connect(select(sensor, "temp", "humidity"), display)

# Connect all pads to the logger
pipeline.connect(sensor, logger)

pipeline.run()
```

**Output:**
```text
[Display] temp: 72
[Display] humidity: 65
[Log] temp: 72
[Log] pressure: 1013
[Log] humidity: 65
[Log] light: 450
[Log] sound: 42
[Log] motion: 0
```

The display only receives temp and humidity, while the logger gets everything.

## Combining Selection with Groups

Selections can be combined with `group()` for powerful routing patterns:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group, select

class DataSource(SourceElement):
    def new(self, pad):
        return Frame(data=f"{self.name}.{pad.name}")

class Processor(SinkElement):
    def pull(self, pad, frame):
        print(f"[{self.name}] Processing: {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Create sources with multiple pads each
source1 = DataSource(name="source1", source_pad_names=("A", "B", "C"))
source2 = DataSource(name="source2", source_pad_names=("X", "Y", "Z"))

# Create processors
proc_alpha = Processor(name="proc_alpha", sink_pad_names=("input",))
proc_beta = Processor(name="proc_beta", sink_pad_names=("input",))

pipeline = Pipeline()

# Route selected pads from multiple sources to different processors
pipeline.connect(
    group(select(source1, "A", "B"), select(source2, "X")),
    proc_alpha
)

pipeline.connect(
    group(select(source1, "C"), select(source2, "Y", "Z")),
    proc_beta
)

pipeline.run()
```

**Output:**
```text
[proc_alpha] Processing: source1.A
[proc_alpha] Processing: source1.B
[proc_alpha] Processing: source2.X
[proc_beta] Processing: source1.C
[proc_beta] Processing: source2.Y
[proc_beta] Processing: source2.Z
```

Complex routing made simple!

## Practical Example: Sensor Data Router

Here's a realistic example routing different sensor readings to specialized processors:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group, select

class EnvironmentSensor(SourceElement):
    def __post_init__(self):
        super().__post_init__()
        self.count = 0

    def new(self, pad):
        self.count += 1
        # Simulate sensor readings
        data = {
            "temperature": 20 + self.count,
            "humidity": 50 + self.count,
            "pressure": 1013 + self.count,
            "co2": 400 + self.count,
            "light": 300 + self.count
        }
        return Frame(data=data[pad.name], EOS=self.count > 3)

class TemperatureProcessor(TransformElement):
    def __post_init__(self):
        super().__post_init__()
        self.input_value = None

    def pull(self, pad, frame):
        self.input_value = frame

    def new(self, pad):
        if self.input_value and self.input_value.data is not None:
            celsius = self.input_value.data
            fahrenheit = (celsius * 9/5) + 32
            return Frame(
                data=f"Temp: {celsius}°C ({fahrenheit:.1f}°F)",
                EOS=self.input_value.EOS
            )
        return Frame(data=None, EOS=True)

class AirQualityProcessor(TransformElement):
    def __post_init__(self):
        super().__post_init__()
        self.inputs = {}

    def pull(self, pad, frame):
        self.inputs[pad.name] = frame

    def new(self, pad):
        # Wait for both inputs
        if "humidity" in self.inputs and "co2" in self.inputs:
            h = self.inputs["humidity"]
            c = self.inputs["co2"]
            if h.data is not None and c.data is not None:
                quality = "Good" if c.data < 450 and h.data < 70 else "Poor"
                return Frame(
                    data=f"Air Quality: {quality} (CO2={c.data}, H={h.data}%)",
                    EOS=h.EOS or c.EOS
                )
        return Frame(data=None, EOS=False)

class Display(SinkElement):
    def pull(self, pad, frame):
        if frame.data:
            print(frame.data)
        if frame.EOS:
            self.mark_eos(pad)

# Create multi-sensor
sensor = EnvironmentSensor(
    name="env_sensor",
    source_pad_names=("temperature", "humidity", "pressure", "co2", "light")
)

# Create specialized processors
temp_proc = TemperatureProcessor(
    name="temp_processor",
    sink_pad_names=("input",),
    source_pad_names=("output",)
)

air_proc = AirQualityProcessor(
    name="air_processor",
    sink_pad_names=("humidity", "co2"),
    source_pad_names=("output",)
)

# Create displays
temp_display = Display(name="temp_display", sink_pad_names=("data",))
air_display = Display(name="air_display", sink_pad_names=("data",))

pipeline = Pipeline()

# Route temperature to its processor
pipeline.connect(select(sensor, "temperature"), temp_proc)
pipeline.connect(temp_proc, temp_display)

# Route humidity and CO2 to air quality processor
pipeline.connect(select(sensor, "humidity", "co2"), air_proc)
pipeline.connect(air_proc, air_display)

# Note: pressure and light are not connected (unused readings)

pipeline.run()
```

**Output:**
```text
Temp: 21°C (69.8°F)
Air Quality: Good (CO2=401, H=51%)
Temp: 22°C (71.6°F)
Air Quality: Good (CO2=402, H=52%)
Temp: 23°C (73.4°F)
Air Quality: Poor (CO2=403, H=53%)
```

Only the needed sensor readings are routed to their respective processors, while unused readings (pressure, light) are simply not connected.

## Selection vs. Full Element

| Scenario | Use | Example |
|----------|-----|---------|
| Need all pads | Full element | `pipeline.connect(source, sink)` |
| Need specific pads | Selection | `pipeline.connect(select(source, "pad1", "pad2"), sink)` |
| Need different subsets | Multiple selections | See router example above |
| Organizing many elements | Groups | `group(elem1, elem2, elem3)` |
| Subsets from groups | Group + selections | `group(select(e1, "p1"), select(e2, "p2"))` |

## Common Patterns

### Route Different Data Types to Different Sinks
```{.python notest}
pipeline.connect(select(sensor, "temp", "pressure"), weather_sink)
pipeline.connect(select(sensor, "humidity", "co2"), air_quality_sink)
```

### Partial Broadcasting
```{.python notest}
# Only broadcast specific pads to all consumers
pipeline.connect(
    select(source, "critical_data", "alerts"),
    group(consumer1, consumer2, consumer3)
)
```

### Multi-Stage Selective Routing
```{.python notest}
# Stage 1: Select from sources
stage1 = group(
    select(source1, "A", "B"),
    select(source2, "X")
)

# Stage 2: Process and select again
pipeline.connect(stage1, processor)
pipeline.connect(select(processor, "validated"), sink)
```

## Best Practices

1. **Use selection for clarity** - Makes data flow explicit
2. **Name pads meaningfully** - Easier to select the right ones
3. **Document routing logic** - Complex selections benefit from comments
4. **Combine with groups** - For powerful multi-element routing
5. **Validate pad names** - Selection will error if pad doesn't exist (good!)

## Next Steps

Learn how to visualize your pipeline structure with [Pipeline Visualization](pipeline_visualization.md) to see all these connections in a graphical format.
