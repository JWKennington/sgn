<!-- index.rst content start -->

# SGN Documentation

SGN is a lightweight Python library for creating and executing task graphs
asynchronously for streaming data. With only builtin-dependencies, SGN is easy to install and use.
This page is for the base library `sgn`, but there is a family of libraries that extend the functionality of SGN,
including:

- [`sgn-ts`](https://git.ligo.org/greg/sgn-ts): TimeSeries utilities for SGN
- [`sgn-ligo`](https://git.ligo.org/greg/sgn-ligo): LSC specific utilities for SGN
- [`sgn-try`](https://git.ligo.org/greg/sgn-try): Process monitoring and alerting utilities for SGN

## Installation

To install SGN, simply run:

```bash
pip install sgn
```

SGN has no dependencies outside of the Python standard library, so it should be easy to install on any
system.

## Quickstart

To get started with SGN, you can create a simple task graph that represents
a simple data processing pipeline with integers. Here's an example:

```python
import functools
from sgn import CallableTransform, CollectSink, IterSource, Pipeline


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
p.insert(
    src,
    trn1,
    snk,
    link_map={
        "t1:sink:H1": "src1:src:H1",
        "snk1:sink:H1": "t1:src:H1",
    },
)

# Run the pipeline
p.run()

# Check the result of the sink queue to see outputs
assert list(snk.collects["snk1:sink:H1"]) == [10, 20, 30]

```

The above example can be modified to use any data type, including json-friendly
nested dictionaries, lists, and strings. The `CallableTransform` class can be used to
create a transform element using any arbitrary function. The `DeqSource` and `DeqSink` classes
are used to create source and sink elements that use `collections.deque` to store data.

## General Concepts

SGN is designed to be simple and easy to use. Here we outline the key concepts, but for more detail see the
key concepts page in the documentation with link: concepts.rst
In SGN there are a few concepts to understand:

### Graph Construction

- **Sources**: Sources are the starting point of a task graph. They produce data that can be consumed by
  other tasks.

- **Transforms**: Transforms are tasks that consume data from one or more sources, process it, and produce new data.

- **Sinks**: Sinks are tasks that consume data from one or more sources and do something with it. This could be writing
  the data to a file, sending it over the network, or anything else.

### Control Flow

Using these concepts, you can create complex task graphs using SGN that process and move data in a variety of ways.
The SGN library provides a simple API for creating and executing task graphs, with a few key types:

- **Frame**: A frame is a unit of data that is passed between tasks in a task graph. Frames can contain any type of
  data, and can be passed between tasks in a task graph.

- **Pad**: A pad is a connection point between two tasks in a task graph. Pads are used to pass frames between tasks,
  and can be used to connect tasks in a task graph. An edge is a connection between two pads in a task graph.

- **Element**: An element is a task in a task graph. Elements can be sources, transforms, or sinks, and can be connected
  together to create a task graph.

- **Pipeline**: A pipeline is a collection of elements that are connected together to form a task graph. Pipelines can
  be executed to process data, and can be used to create complex data processing workflows.
