# SGN Documentation

SGN is a lightweight Python library for creating and executing task graphs
asynchronously for streaming data. With only builtin-dependencies, SGN is easy to install and use.
This page is for the base library `sgn`, but there is a family of libraries that extend the functionality of SGN,
including:

- [`sgn-ts`](https://git.ligo.org/greg/sgn-ts): TimeSeries utilities for SGN
- [`sgn-ligo`](https://git.ligo.org/greg/sgn-ligo): LSC specific utilities for SGN

## Installation

For the latest development version, install directly from git:

```bash
pip install git+https://git.ligo.org/greg/sgn.git
```

Or for a stable release:

```bash
pip install sgn
```

SGN has no dependencies outside of the Python standard library, so it should be easy to install on any
system. That being said, developers must install extra packages to help with development, please see below.

??? info "Developer Installation"
    For developers who want to contribute or work with the full development environment, install with the `dev` extras:

    ```bash
    pip install "git+https://git.ligo.org/greg/sgn.git#egg=sgn[dev]"
    ```

    Or for a local editable installation:

    ```bash
    git clone https://git.ligo.org/greg/sgn.git
    cd sgn
    pip install -e ".[dev]"
    ```

    The `dev` extras include:

    - **docs**: MkDocs and extensions for building documentation
    - **lint**: Code formatting and linting tools (black, flake8, mypy, etc.)
    - **test**: Testing tools (pytest, pytest-cov, pytest-markdown-docs)
    - **monitoring**: Performance monitoring tools (psutil)

    After installation, run `make` to verify your development environment:

    ```bash
    make
    ```

    This will run formatting, linting, type checking, and tests to ensure everything is set up correctly.

    This is a 100% coverage package and we hold ourselves to that standard. No MRs will be accepted without
    make passing and maintaining 100% coverage. Here is something like what you should see after make

    ```bash
    --------- coverage: platform darwin, python 3.11.11-final-0 ----------
    Name                    Stmts   Miss   Cover   Missing
    ------------------------------------------------------
    src/sgn/apps.py           180      0  100.0%
    src/sgn/base.py           142      0  100.0%
    src/sgn/control.py        113      0  100.0%
    src/sgn/frames.py          19      0  100.0%
    src/sgn/groups.py          96      0  100.0%
    src/sgn/logger.py          43      0  100.0%
    src/sgn/profile.py         59      0  100.0%
    src/sgn/sinks.py           45      0  100.0%
    src/sgn/sources.py        202      0  100.0%
    src/sgn/subprocess.py     214      0  100.0%
    src/sgn/transforms.py      67      0  100.0%
    src/sgn/visualize.py       59      0  100.0%
    ------------------------------------------------------
    TOTAL                    1239      0  100.0%
    ```

## Overview

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
  ┌───────────────────────────────────────────────────────────────────┐
  │                    Pipeline Event Loop (Repeats)                  │
  │  ┌─────────────┐                                                  │
  │  │            \ /                                                 │
  │  │             v                                                  │
  │  │   ┌──────────────────────┐                                     │
  │  │   │  Source Element      │  Produces data and sends            │
  │  │   │                      │  it downstream via source pads      │
  │  │   │   [ source pad ]     │                                     │
  │  │   └─────────┬────────────┘                                     │
  │  │             │                                                  │
  │  │             │  Frame                                           │
  │  │             ▼                                                  │
  │  │   ┌─────────┴────────────┐                                     │
  │  │   │   [ sink pad ]       │  Receives data, processes it,       │
  │  │   │                      │  and sends it to source pads        │
  │  │   │  Transform Element   │                                     │
  │  │   │                      │                                     │
  │  │   │   [ source pad ]     │                                     │
  │  │   └─────────┬────────────┘                                     │
  │  │             │                                                  │
  │  │             │  Frame                                           │
  │  │             ▼                                                  │
  │  │   ┌─────────┴────────────┐                                     │
  │  │   │   [ sink pad ]       │  Consumes data from upstream        │
  │  │   │                      │  (write to file, network, etc.)     │
  │  │   │  Sink Element        │                                     │
  │  │   │                      │                                     │
  │  │   └──────────────────────┘                                     │
  │  │     ,                                                          │
  │  └─── <  Loop repeats: Each iteration pulls Frames through the    │
  │        `               graph until end of stream (EOS)            │
  └───────────────────────────────────────────────────────────────────┘
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

## Getting Started

Please see the [Tutorials](tutorials/) for step-by-step examples and detailed usage information.


