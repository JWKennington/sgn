# SGN Documentation

SGN is a lightweight Python library for creating and executing task graphs
asynchronously for streaming data. With only builtin-dependencies, SGN is easy to install and use.
This page is for the base library `sgn`, but there is a family of libraries that extend the functionality of SGN,
including:

- [`sgn-ts`](https://git.ligo.org/greg/sgn-ts): TimeSeries utilities for SGN
- [`sgn-ligo`](https://git.ligo.org/greg/sgn-ligo): LSC specific utilities for SGN

## Installation

To install SGN, simply run:

```bash
pip install sgn
```

SGN has no dependencies outside of the Python standard library, so it should be easy to install on any
system.

## Quick Start

SGN will execute a directed acyclic graph of ["Source
Pads"](api/base/#sgn.base.SourcePad) that produce data and ["Sink
Pads"](api/base/#sgn.base.SinkPad) that receive data in
["Frames"](api/base/#sgn.base.Frame). Pads provide asynchronous function calls
bound to classes called ["Source Elements"](api/base/#sgn.base.SourceElement),
["Transform Elements"](api/base/#sgn.base.TransformElement), and ["Sink
Elements"](api/base/#sgn.base.SinkElement). Collections of elements arranged in
a graph along with the event loop are contained in a
["Pipeline"](api/base/#sgn.apps.Pipeline) Data must have an origin (Source) and
a end point (Sink) in all graphs. 

```
            ----------------------
    v      |                      |      <
   /       |   Source Element 1   |       \
  /        |                      |        \
 /          ---[source pad 'a']---          \
|                     |                      | The event loop runs this graph over and
|                     |                      | over pulling data through the pads
|           --- [sink pad 'b'] ---           |
|          |                      |          | The collection of elements and event 
|          |  Transform Element 1 |          | loop is managed by a Pipeline class
|          |                      |          |
|           ---[source pad 'b']---           |
|                     |                      |
|                     |                      |
|                    ...                     |
|                    ...                     |
|                     |                      |
|                     | data flow            | 
 \                    V                      | 
  \         --- [sink pad 'x'] ---          /
   \       |                      |        /
    \      |   Sink Element 1     |       /
     >     |                      |      ^
            ----------------------       
```

### Key concepts

- **Sources**: Sources are the starting point of a task graph. They produce data that can be consumed by
  other tasks.

- **Transforms**: Transforms are tasks that consume data from one or more sources, process it, and produce new data.

- **Sinks**: Sinks are tasks that consume data from one or more sources and do something with it. This could be writing
  the data to a file, sending it over the network, or anything else.

- **Frame**: A frame is a unit of data that is passed between tasks in a task graph. Frames can contain any type of
  data, and can be passed between tasks in a task graph.

- **Pad**: A pad is a connection point between two tasks in a task graph. Pads are used to pass frames between tasks,
  and can be used to connect tasks in a task graph. An edge is a connection between two pads in a task graph.

- **Element**: An element is a task in a task graph. Elements can be sources, transforms, or sinks, and can be connected
  together to create a task graph.

- **Pipeline**: A pipeline is a collection of elements that are connected together to form a task graph. Pipelines can
  be executed to process data, and can be used to create complex data processing workflows.


### Hello World

Here is a simple example that passes the string "hello" as data for every execution of the event loop


```{.python notest}
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class MySourceClass(SourceElement):
    def new(self, pad):
        return Frame(data="hello")

class MySinkClass(SinkElement):
    def pull(self, pad, frame):
        print (frame.data)

source = MySourceClass(source_pad_names = ("a",))
sink = MySinkClass(sink_pad_names = ("x",))

pipeline = Pipeline()

pipeline.insert(source, sink, link_map = {sink.snks["x"]: source.srcs["a"]})

pipeline.run()
```

If you run this, it will run forever and you will see

```
hello
hello
hello
hello
hello
hello
hello
hello
hello
hello
...
```


Please see [Tutorials](tutorials/) for more information.


