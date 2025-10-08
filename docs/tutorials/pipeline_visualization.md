# Pipeline Visualization

Complex pipelines can be difficult to understand from code alone. SGN's `visualize()` function generates graphical diagrams of your pipeline structure, making it easy to debug connections, document your architecture, and communicate designs.

## Prerequisites

The visualization feature requires the `graphviz` package:

```bash
pip install graphviz
```

You'll also need the Graphviz system library installed:

- **Ubuntu/Debian**: `sudo apt-get install graphviz`
- **macOS**: `brew install graphviz`
- **Windows**: Download from [graphviz.org](https://graphviz.org/download/)

## Basic Visualization

The simplest way to visualize a pipeline:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline

class DataSource(SourceElement):
    def new(self, pad):
        return Frame(data="data")

class Processor(TransformElement):
    def pull(self, pad, frame):
        pass

    def new(self, pad):
        return Frame(data="processed")

class DataSink(SinkElement):
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

# Build pipeline
source = DataSource(name="source", source_pad_names=("out",))
processor = Processor(name="processor", sink_pad_names=("in",), source_pad_names=("out",))
sink = DataSink(name="sink", sink_pad_names=("in",))

pipeline = Pipeline()
pipeline.connect(source, processor)
pipeline.connect(processor, sink)

# Visualize it!
graph = pipeline.visualize()
graph.view()  # Opens in your default viewer
```

This creates a diagram showing your pipeline flow with all elements and connections.

## Understanding the Diagram

The visualization uses colors to show pad states:

- **Light Green (MediumAquaMarine)** - Source pads (data producers)
- **Light Blue** - Sink pads (data consumers)
- **Red (Tomato)** - Unconnected pads (potential issues!)
- **Blue boxes** - Elements

Arrows show data flow direction from source pads to sink pads.

## Saving to Files

You can save diagrams in various formats:

```{.python notest}
# Save as PNG
pipeline.visualize(path="my_pipeline.png")

# Save as PDF
pipeline.visualize(path="my_pipeline.pdf")

# Save as SVG (scalable)
pipeline.visualize(path="my_pipeline.svg")
```

The format is automatically determined from the file extension.

## Adding Labels

Add a descriptive label to your diagram:

```{.python notest}
graph = pipeline.visualize(label="Sensor Data Processing Pipeline")
graph.view()
```

The label appears at the top of the diagram in large, bold text.

## Debugging with Visualization

Visualization is especially powerful for debugging connection issues.

### Finding Unconnected Pads

Red pads indicate missing connections:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class MultiSource(SourceElement):
    def new(self, pad):
        return Frame(data=f"From {pad.name}")

class Sink(SinkElement):
    def pull(self, pad, frame):
        print(frame.data)
        if frame.EOS:
            self.mark_eos(pad)

# Source has 3 pads, but we only connect 2
source = MultiSource(name="source", source_pad_names=("A", "B", "C"))
sink = Sink(name="sink", sink_pad_names=("A", "B"))

pipeline = Pipeline()
pipeline.connect(source, sink)

# Visualize to find the problem
graph = pipeline.visualize(label="Finding Unconnected Pads")
graph.view()
```

In the diagram, you'll see:
- Green: `source.A` and `source.B` (connected)
- **Red: `source.C`** (unconnected - this is your issue!)
- Blue: `sink.A` and `sink.B` (connected)

The red pad immediately shows you that `source.C` is producing data that's going nowhere.

### Verifying Complex Routing

For pipelines with many elements and connections, visualization confirms your routing logic:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group, select

class Sensor(SourceElement):
    def new(self, pad):
        return Frame(data=f"{self.name}.{pad.name}")

class Logger(SinkElement):
    def pull(self, pad, frame):
        print(frame.data)
        if frame.EOS:
            self.mark_eos(pad)

# Create multiple sensors with multiple pads
sensor1 = Sensor(name="sensor1", source_pad_names=("temp", "pressure", "humidity"))
sensor2 = Sensor(name="sensor2", source_pad_names=("light", "sound"))

# Create specialized loggers
temp_logger = Logger(name="temp_log", sink_pad_names=("data",))
env_logger = Logger(name="env_log", sink_pad_names=("data",))

# Complex routing
pipeline = Pipeline()
pipeline.connect(select(sensor1, "temp"), temp_logger)
pipeline.connect(
    group(
        select(sensor1, "pressure", "humidity"),
        sensor2
    ),
    env_logger
)

# Visualize to verify routing
graph = pipeline.visualize(label="Multi-Sensor Routing")
graph.render("sensor_routing.png", view=True)
```

The diagram clearly shows:
- `sensor1.temp` → `temp_log.data`
- `sensor1.pressure` → `env_log.data`
- `sensor1.humidity` → `env_log.data`
- `sensor2.light` → `env_log.data`
- `sensor2.sound` → `env_log.data`

You can instantly verify that your routing logic is correct!

## Practical Example: Complete Pipeline Documentation

Use visualization as documentation for complex systems:

```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, TransformElement, SinkElement, Frame
from sgn.apps import Pipeline
from sgn.groups import group

class DataCollector(SourceElement):
    def new(self, pad):
        return Frame(data=f"Raw data from {pad.name}")

class Validator(TransformElement):
    def pull(self, pad, frame):
        self.input = frame

    def new(self, pad):
        return self.input

class Normalizer(TransformElement):
    def pull(self, pad, frame):
        self.input = frame

    def new(self, pad):
        return self.input

class Database(SinkElement):
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

class Cache(SinkElement):
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

# Build a multi-stage pipeline
collector = DataCollector(name="data_collector", source_pad_names=("sensor_feed",))
validator = Validator(name="validator", sink_pad_names=("raw",), source_pad_names=("validated",))
normalizer = Normalizer(name="normalizer", sink_pad_names=("data",), source_pad_names=("normalized",))
database = Database(name="database", sink_pad_names=("input",))
cache = Cache(name="cache", sink_pad_names=("input",))

pipeline = Pipeline()
pipeline.connect(collector, validator)
pipeline.connect(validator, normalizer)
pipeline.connect(normalizer, group(database, cache))

# Generate documentation diagram
graph = pipeline.visualize(
    label="Production Data Pipeline v2.0",
    path="docs/architecture/data_pipeline.pdf"
)

print("Pipeline diagram saved to docs/architecture/data_pipeline.pdf")
```

Now your documentation has a clear, accurate diagram of your pipeline architecture!

## Visualization in Development Workflow

### 1. Design Phase
Sketch your pipeline in code, then visualize to validate the design before implementation:

```{.python notest}
# Rough pipeline sketch
pipeline = Pipeline()
pipeline.connect(sources, processors)
pipeline.connect(processors, sinks)

# Does this match your mental model?
pipeline.visualize(label="Initial Design").view()
```

### 2. Development Phase
As you build, visualize frequently to catch connection issues early:

```{.python notest}
# After each major connection
pipeline.connect(new_element, existing_element)
pipeline.visualize().view()  # Quick check
```

### 3. Review Phase
Include diagrams in code reviews:

```{.python notest}
# Generate diagram for review
pipeline.visualize(
    label=f"{feature_name} Pipeline",
    path=f"docs/reviews/{feature_name}.png"
)
```

### 4. Documentation Phase
Update architectural docs when the pipeline changes:

```{.python notest}
# Update architecture diagrams
pipeline.visualize(
    label=f"System Architecture - {version}",
    path=f"docs/architecture/pipeline_{version}.pdf"
)
```

## Tips and Tricks

### Naming Matters
Element and pad names appear in the diagram, so use clear, descriptive names:

```{.python notest}
# Good - clear purpose
sensor = TemperatureSensor(name="living_room_temp", ...)

# Bad - unclear
sensor = TemperatureSensor(name="s1", ...)
```

### Large Pipelines
For very large pipelines, save as SVG for scalability:

```{.python notest}
pipeline.visualize(path="large_pipeline.svg")
```

SVG files can be zoomed without quality loss.

### Iterative Debugging
When debugging, use visualization in a loop:

```{.python notest}
# Build incrementally and visualize each step
pipeline.connect(source1, sink1)
pipeline.visualize(label="Step 1").view()

pipeline.connect(source2, sink1)
pipeline.visualize(label="Step 2").view()

# And so on...
```

### Sharing Diagrams
Export to PNG for easy sharing in emails/chat:

```{.python notest}
pipeline.visualize(path="pipeline_diagram.png")
# Attach pipeline_diagram.png to your message
```

## Best Practices

1. **Visualize early and often** - Catch connection issues before running
2. **Add labels** - Context helps others understand your pipeline
3. **Check for red pads** - They indicate forgotten connections
4. **Use in documentation** - A picture is worth a thousand lines of code
5. **Version your diagrams** - Track how your pipeline evolves
6. **Include in CI/CD** - Auto-generate diagrams for documentation

## Troubleshooting

### GraphViz Not Found
```
Error: GraphViz not installed
```
**Solution**: Install the graphviz system package (not just the Python package)

### Display Issues
If `.view()` doesn't work, save to a file instead:
```{.python notest}
pipeline.visualize(path="pipeline.png")
# Then open pipeline.png manually
```

### Too Complex
If the diagram is overwhelming:
- Break your pipeline into sub-pipelines
- Visualize each section separately
- Use better element/pad naming

## Summary

Pipeline visualization is a powerful tool for:
- **Debugging** - Find unconnected pads (red) instantly
- **Documentation** - Generate accurate architecture diagrams
- **Communication** - Show pipeline structure to teammates
- **Development** - Validate designs before full implementation

Make visualization part of your workflow, and you'll build better pipelines faster!

## Next Steps

You now have a complete understanding of SGN's pipeline management features! Explore the [Advanced Tutorials](../tutorials/) to learn about HTTP control, subprocess parallelization, and more.
