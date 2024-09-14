# Example: Simple Pipeline

This example shows a simple pipeline for doing arithmetic with scalars. The pipeline containing a source, a transform, 
and a sink. 

The source is a `IterSource`, which iterates through the given list and produces one `Frame` object
per item in the list. 

For the transform, we define a function `scale` that multiplies the input by a given factor. To use this function
in the pipeline, we create a `CallableTransform` element that wraps the function, by using the helper method
`CallableTransform.from_callable`. Note that we use `functools.partial` to set the `factor` argument to 10.

The sink is a `CollectSink`, which consumes the `Frame` objects produced by the source, appending them to a list.

## Code

```python
import functools
from sgn import Pipeline, CollectSink, IterSource, CallableTransform

# Define a function to use in the pipeline
def scale(frame, factor: float):
    return None if frame.data is None else frame.data * factor

# Create source element
src = IterSource(
    name="src1",
    source_pad_names=["H1"],
    iters={"src1:src:H1": [1, 2, 3]},
)

# Create a transform element using an arbitrary function
trn1 = CallableTransform.from_callable(
    name="t1",
    sink_pad_names=["H1"],
    callable=functools.partial(scale, factor=10),
    output_name="H1",
)

# Create the sink so we can access the data after running
snk = CollectSink(
    name="snk1",
    sink_pad_names=("H1",),
)

# Create the Pipeline
p = Pipeline()

# Insert elements into pipeline and link them explicitly
p.insert(src, trn1, snk, link_map={
    "t1:sink:H1": "src1:src:H1",
    "snk1:sink:H1": "t1:src:H1",
})

# Run the pipeline
p.run()

# Check the result of the sink queue to see outputs
assert list(snk.collects["snk1:sink:H1"]) == [10, 20, 30]
```
