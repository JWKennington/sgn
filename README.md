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

At the moment only single src / sink elements are supported for trivial linear graphs.  There is a skeleton to start expanding it and it is written such that hopefully it will be possible.

## Getting started

Clone the repo and run ```./test```

```
e1-054675:greg-fw channa$ ./test 
{'cnt': 1, 'name': 'transform'}
{'cnt': 2, 'name': 'transform'}
{'cnt': 3, 'name': 'transform'}
{'cnt': 4, 'name': 'transform'}
{'cnt': 5, 'name': 'transform'}
{'cnt': 6, 'name': 'transform'}
{'cnt': 7, 'name': 'transform'}
{'cnt': 8, 'name': 'transform'}
{'cnt': 9, 'name': 'transform'}
{'cnt': 10, 'name': 'transform'}
{'cnt': 11, 'name': 'transform'}
...
```
