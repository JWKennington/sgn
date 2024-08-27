# Core Concepts

This page provides a detailed overview of the key concepts in `sgn`, including the following:

- [**Frame**](#frame): The basic building blocks of `sgn` data.
- [**Pad**](#pad): The primary nodes in the task graph.
- [**Element**](#element): Small, thematic groupings of pads, to make graph construction easier.
- [**Pipeline**](#pipeline): The task graph, capable of asynchronous execution.

## Frame

A `Frame` is the basic building block of `sgn` data. It is an object with arbitrary attributes.
Frames are used to represent data in `sgn`, and they are passed between pads in the task graph.
All frames have a few properties:

- `metadata`: A dictionary of metadata associated with the frame.
- `EOS`: A boolean flag indicating the end of the stream.
- `data`: An arbitrary python object to be manipulated in the stream

## Pad

A `Pad` is a node in the task graph. It possesses a callable that can be applied to a `Frame`, and it can be
connected (linked) to other pads in the graph.

## Element

An `Element` is a small, thematic grouping of pads. It is used to make graph construction easier. An element has three
types of pads:

- Source pads (Output Pads): Pads that produce data, called source pads or output pads.
- Sink pads (Input Pads): Pads that consume data, called sink pads or input pads.
- Internal pads: Pads that are used for actions inside an element.

```{note}
The terms "source" and "sink" here belong to a convention inherited from streaming frameworks like GStreamer, and
reflect the direction of data flow. In `sgn`, the terms "output" and "input" are used interchangeably with "source" and
"sink".
```

## Pipeline

A `Pipeline` is the task graph in `sgn`. It is a directed acyclic graph (DAG) that represents the flow of data between
pads. The pipeline is capable of asynchronous execution, and it can be used to build complex data processing workflows.

