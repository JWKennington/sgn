# sgn

This is an attempt to sketch a streaming framework to replace some good aspects of gstlal.

There are the following concepts defined

- **Buffer**: An object designed to carry data through some execution graph
- **Source Pad**: An object that is designed to create a buffer when asked
- **Sink Pad**: An object that will receive a buffer when asked
- **Source Elements**: These manage the creation of buffers on one or more Pads with the assumption that they will be executed as part of a graph
- **Sink Elements**: These manage the consumption of buffers on one or more Pads with the assumption that they will be executed as part of a graph
- **Transform Elements**: These manage the consumption of buffers that immediately result in the production of new buffers.
- **Pipelines**: These manage the graph execution to pull buffers through the graph and potentially handle other events.

## limitations

NONE

## Getting started

Clone the repo and run

```
$ python tests/test_graph.py 
buffer flow:  'src1:H1:src' -> 'H1trans1:H1:src' -> 'snk1:H1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans1:L1:src' -> 'snk1:L1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans2:L1:src' -> 'snk2:L1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:V1:src' -> 'snk2:V1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:K1:src' -> 'snk2:K1:sink'
buffer flow:  'src1:H1:src' -> 'H1trans1:H1:src' -> 'snk1:H1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans1:L1:src' -> 'snk1:L1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans2:L1:src' -> 'snk2:L1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:V1:src' -> 'snk2:V1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:K1:src' -> 'snk2:K1:sink'
buffer flow:  'src1:H1:src' -> 'H1trans1:H1:src' -> 'snk1:H1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans1:L1:src' -> 'snk1:L1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans2:L1:src' -> 'snk2:L1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:V1:src' -> 'snk2:V1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:K1:src' -> 'snk2:K1:sink'
```
