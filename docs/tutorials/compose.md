# Composing Elements

SGN's composition system lets you combine multiple elements into a single
reusable unit. This hides internal complexity and enables code reuse.

## Composition Types

| Type | Input Elements | Result |
|------|---------------|--------|
| `ComposedSourceElement` | Source + Transform(s) | Acts as a SourceElement |
| `ComposedTransformElement` | Transform(s) | Acts as a TransformElement |
| `ComposedSinkElement` | Transform(s) + Sink | Acts as a SinkElement |

## Example 1: Basic Composition

The `Compose` class mirrors Pipeline's API with `connect()` for building
compositions.

```python
from sgn import Pipeline, Compose, IterSource, CollectSink
from sgn.transforms import CallableTransform

# Create elements
source = IterSource(
    name="src",
    source_pad_names=["data"],
    iters={"data": iter([1, 2, 3, 4, 5])},
    eos_on_empty={"data": True},
)

double = CallableTransform.from_callable(
    name="double",
    callable=lambda frame: frame.data * 2 if frame.data is not None else None,
    output_pad_name="data",
    sink_pad_names=["data"],
)

add_ten = CallableTransform.from_callable(
    name="add_ten",
    callable=lambda frame: frame.data + 10 if frame.data is not None else None,
    output_pad_name="data",
    sink_pad_names=["data"],
)

# Compose: source -> double -> add_ten
composed_source = (
    Compose()
    .connect(source, double)
    .connect(double, add_ten)
    .as_source(name="processed_source")
)

# Use like any regular source
sink = CollectSink(name="sink", sink_pad_names=["data"])
pipeline = Pipeline()
pipeline.connect(composed_source, sink)
pipeline.run()

print(f"Input:  [1, 2, 3, 4, 5]")
print(f"Output: {list(sink.collects['data'])}")
# Each value: doubled then +10
assert list(sink.collects["data"]) == [12, 14, 16, 18, 20]
```

## Example 2: All Composition Types

This example demonstrates `ComposedSourceElement`, `ComposedTransformElement`,
and `ComposedSinkElement` working together in a single pipeline.

```python
from sgn import Pipeline, Compose, IterSource, CollectSink
from sgn.transforms import CallableTransform

# --- ComposedSourceElement: source that doubles values ---
raw_source = IterSource(
    name="raw",
    source_pad_names=["data"],
    iters={"data": iter([1, 2, 3, 4, 5])},
)
double_transform = CallableTransform.from_callable(
    name="double",
    callable=lambda frame: frame.data * 2 if frame.data is not None else None,
    output_pad_name="data",
    sink_pad_names=["data"],
)
composed_source = (
    Compose()
    .connect(raw_source, double_transform)
    .as_source("doubled_source")
)

# --- ComposedTransformElement: adds 100 then negates ---
add_100 = CallableTransform.from_callable(
    name="add_100",
    callable=lambda frame: frame.data + 100 if frame.data is not None else None,
    output_pad_name="data",
    sink_pad_names=["data"],
)
negate = CallableTransform.from_callable(
    name="negate",
    callable=lambda frame: -frame.data if frame.data is not None else None,
    output_pad_name="data",
    sink_pad_names=["data"],
)
composed_transform = (
    Compose()
    .connect(add_100, negate)
    .as_transform("add_and_negate")
)

# --- ComposedSinkElement: formats before collecting ---
formatter = CallableTransform.from_callable(
    name="format",
    callable=lambda frame: f"result={frame.data}" if frame.data is not None else None,
    output_pad_name="data",
    sink_pad_names=["data"],
)
collector = CollectSink(name="collector", sink_pad_names=["data"])
composed_sink = (
    Compose()
    .connect(formatter, collector)
    .as_sink("formatting_sink")
)

# --- Pipeline: composed_source -> composed_transform -> composed_sink ---
pipeline = Pipeline()
pipeline.connect(composed_source, composed_transform)
pipeline.connect(composed_transform, composed_sink)
pipeline.run()

print("Pipeline: source(double) -> transform(+100, negate) -> sink(format)")
print(f"Input:  [1, 2, 3, 4, 5]")
print(f"Output: {list(collector.collects['data'])}")
# Each value: doubled, +100, negated, formatted
# 1 -> 2 -> 102 -> -102 -> "result=-102"
assert list(collector.collects["data"]) == [
    "result=-102", "result=-104", "result=-106", "result=-108", "result=-110"
]
```

## Example 3: Multiple Sources Merging

Use `connect()` to build non-linear compositions where multiple sources
feed into a single transform.

```python
from sgn import Pipeline, Compose, IterSource, CollectSink
from sgn.transforms import CallableTransform

# Two independent data streams
temperatures = IterSource(
    name="temps",
    source_pad_names=["temp"],
    iters={"temp": iter([20, 25, 30])},
    eos_on_empty={"temp": True},
)

humidities = IterSource(
    name="humid",
    source_pad_names=["humid"],
    iters={"humid": iter([50, 60, 70])},
    eos_on_empty={"humid": True},
)

# Transform that processes both streams (converts units)
converter = CallableTransform.from_combinations(
    name="convert",
    combos=[
        # Celsius to Fahrenheit
        (("temp",), lambda f: f.data * 9/5 + 32 if f.data is not None else None, "temp"),
        # Humidity stays the same but add % symbol
        (("humid",), lambda f: f"{f.data}%" if f.data is not None else None, "humid"),
    ],
)

# Compose non-linear graph: two sources -> one transform
weather_source = (
    Compose()
    .connect(temperatures, converter)
    .connect(humidities, converter)
    .as_source("weather_data")
)

sink = CollectSink(name="sink", sink_pad_names=["temp", "humid"])
pipeline = Pipeline()
pipeline.connect(weather_source, sink)
pipeline.run()

print("Non-linear composition: two sources merged into one")
print(f"Temperatures (C->F): {list(sink.collects['temp'])}")
print(f"Humidities:          {list(sink.collects['humid'])}")
assert list(sink.collects["temp"]) == [68.0, 77.0, 86.0]
assert list(sink.collects["humid"]) == ["50%", "60%", "70%"]
```

## Example 4: Fan-Out to Multiple Sinks

Compose a single input that fans out to multiple sinks.

```python
from sgn import Pipeline, Compose, IterSource, CollectSink
from sgn.transforms import CallableTransform

# Transform that produces multiple outputs from one input
analyzer = CallableTransform.from_combinations(
    name="analyze",
    combos=[
        (("data",), lambda f: f.data ** 2 if f.data is not None else None, "squared"),
        (("data",), lambda f: f.data ** 0.5 if f.data is not None else None, "sqrt"),
    ],
)

squared_sink = CollectSink(name="sq_sink", sink_pad_names=["squared"])
sqrt_sink = CollectSink(name="sqrt_sink", sink_pad_names=["sqrt"])

# Compose: one transform fanning out to two sinks
analysis_sink = (
    Compose()
    .connect(analyzer, squared_sink)
    .connect(analyzer, sqrt_sink)
    .as_sink("dual_analysis")
)

source = IterSource(
    name="numbers",
    source_pad_names=["data"],
    iters={"data": iter([4, 9, 16])},
    eos_on_empty={"data": True},
)

pipeline = Pipeline()
pipeline.connect(source, analysis_sink)
pipeline.run()

print("Fan-out composition: one input to multiple sinks")
print(f"Input:   {[4, 9, 16]}")
print(f"Squared: {list(squared_sink.collects['squared'])}")
print(f"Sqrt:    {list(sqrt_sink.collects['sqrt'])}")
assert list(squared_sink.collects["squared"]) == [16, 81, 256]
assert list(sqrt_sink.collects["sqrt"]) == [2.0, 3.0, 4.0]
```

## Example 5: Reusable Factory Pattern

Create factory functions that return parameterized composed elements.

```python
from sgn import Pipeline, Compose, IterSource, CollectSink
from sgn.transforms import CallableTransform


def create_scale_and_offset(name: str, scale: float, offset: float):
    """Factory that creates a composed transform: (x * scale) + offset."""
    multiply = CallableTransform.from_callable(
        name=f"{name}_mult",
        callable=lambda frame: frame.data * scale if frame.data is not None else None,
        output_pad_name="data",
        sink_pad_names=["data"],
    )
    add = CallableTransform.from_callable(
        name=f"{name}_add",
        callable=lambda frame: frame.data + offset if frame.data is not None else None,
        output_pad_name="data",
        sink_pad_names=["data"],
    )
    return Compose().connect(multiply, add).as_transform(name)


# Create two different scaling transforms
to_fahrenheit = create_scale_and_offset("to_f", scale=9/5, offset=32)  # C -> F
to_kelvin = create_scale_and_offset("to_k", scale=1, offset=273.15)    # C -> K

# Test with temperature in Celsius
source = IterSource(
    name="celsius",
    source_pad_names=["data"],
    iters={"data": iter([0, 20, 100])},
)
sink = CollectSink(name="sink", sink_pad_names=["data"])

pipeline = Pipeline()
pipeline.connect(source, to_fahrenheit)
pipeline.connect(to_fahrenheit, sink)
pipeline.run()

print(f"Celsius:    [0, 20, 100]")
print(f"Fahrenheit: {list(sink.collects['data'])}")
assert list(sink.collects["data"]) == [32.0, 68.0, 212.0]
```

## Best Practices

1. **Handle None data**: Transforms should handle `None` (appears in EOS frames)
2. **Use factory functions**: For reusable patterns with independent state
3. **Name compositions**: Meaningful names help with debugging and visualization
4. **`connect()` auto-inserts**: You don't need to call `insert()` - `connect()`
   automatically inserts elements that aren't yet in the composition

## Summary

The `Compose` class mirrors Pipeline's API:

| Compose | Pipeline |
|---------|----------|
| `insert()` | `insert()` |
| `connect()` | `connect()` |

Use `connect()` to build both linear chains and non-linear graphs (merging,
fan-out). Finalize with `as_source()`, `as_transform()`, or `as_sink()`.
