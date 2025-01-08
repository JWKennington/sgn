# Example: Simple JSON Pipeline

This example shows a simple pipeline for performing operations on more complicated payloads, similar to JSON-style
data structures. The pipeline containing a source, a transform, and a sink. 

The source is a `IterSource`, which iterates through the given list `payloads` and produces one `IterFrame` object
per item in the list. Note that the list contains two items, each of which is a list of dictionaries. Each dictionary
represents a packet of data, with a `time` field, a `buffer` field (a numpy array), and a `trusted` field (a boolean).
This shows that the `IterFrame` object can contain complex data structures, including an iterable of dictionaries, 
perhaps representing a "recent" poll of a JSON-style source service.

For the transform, we define a function `demean_if_trusted` that subtracts the mean value from the `buffer` array, if and 
only if the data packet is "trusted", which is stored in the `trusted` key of the payload. While simple, this shows
how to build more complex operators that can depend on multiple features of a payload. To use this function
in the pipeline, we create a `CallableTransform` element that wraps the function, by using the helper method
`CallableTransform.from_callable`. Note that we use `functools.partial`.

The sink is a `CollectSink`, which consumes the `Frame` objects produced by the source, appending them to a list.

## Code

```python
import datetime
import numpy
from sgn import Pipeline, IterSource, CollectSink, CallableTransform, IterFrame

# Define the payloads
payloads = [
    # Payload 1, one trusted one not
    [
        {"time": datetime.datetime.strptime("2021-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
         "buffer": numpy.array([1., 2., 3.]),
         "trusted": True},
        {"time": datetime.datetime.strptime("2021-01-01T00:00:01", "%Y-%m-%dT%H:%M:%S"),
         "buffer": numpy.array([1., numpy.nan, 3.]),
         "trusted": False},
    ],
    # Payload 2, both trusted
    [
        {"time": datetime.datetime.strptime("2021-01-01T00:00:02", "%Y-%m-%dT%H:%M:%S"),
         "buffer": numpy.array([4., 5., 6.]),
         "trusted": True},
        {"time": datetime.datetime.strptime("2021-01-01T00:00:03", "%Y-%m-%dT%H:%M:%S"),
         "buffer": numpy.array([7., 8., 9.]),
         "trusted": True},
    ],
]


# Define a function to use in the pipeline
def demean_if_trusted(frame: IterFrame):
    if frame.data is None:
        return None

    results = []
    for packet in frame.data:
        new_packet = packet.copy()
        if new_packet["trusted"]:
            new_packet["buffer"] -= numpy.mean(new_packet["buffer"])
        results.append(new_packet)
    return results


# Create source element
src = IterSource(
    name="src1",
    source_pad_names=["H1"],
    iters={"src1:src:H1": payloads},
    frame_factory=IterFrame,
)

# Create a transform element using an arbitrary function
trn1 = CallableTransform.from_callable(
    name="t1",
    sink_pad_names=["H1"],
    callable=demean_if_trusted,
    output_pad_name="H1",
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
    "t1:snk:H1": "src1:src:H1",
    "snk1:snk:H1": "t1:src:H1",
})

# Run the pipeline
p.run()

# Check the result of the sink queue to see outputs
# We check each packet individually to avoid numpy array comparison issues
result = list(snk.collects["snk1:snk:H1"])
expected = [
    [
        {"time": datetime.datetime(2021, 1, 1, 0, 0, 0),
         "buffer": numpy.array([-1., 0., 1.]),
         "trusted": True},
        {"time": datetime.datetime(2021, 1, 1, 0, 0, 1),
         "buffer": numpy.array([1., numpy.nan, 3.]),
         "trusted": False},
    ],
    [
        {"time": datetime.datetime(2021, 1, 1, 0, 0, 2),
         "buffer": numpy.array([-1., 0., 1.]),
         "trusted": True},
        {"time": datetime.datetime(2021, 1, 1, 0, 0, 3),
         "buffer": numpy.array([-1., 0., 1.]),
         "trusted": True},
    ]
]
for r, e in zip(result, expected):
    for rp, ep in zip(r, e):
        assert rp["time"] == ep["time"]
        assert numpy.allclose(rp["buffer"], ep["buffer"], equal_nan=True), f"{rp['buffer']} != {ep['buffer']}"
        assert rp["trusted"] == ep["trusted"]
```
