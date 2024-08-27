SGN Documentation
=================

SGN is a lightweight Python library for creating and executing task graphs
asynchronously for streaming data. With only builtin-dependencies, SGN is easy to install and use.


Installation
------------

To install SGN, simply run:

.. code-block:: bash

    pip install sgn

SGN has no dependencies outside of the Python standard library, so it should be easy to install on any
system.

General Concepts
----------------

SGN is designed to be simple and easy to use. Here we outline the key concepts, but for more detail see the
key concepts page in the documentation with link: concepts.rst
In SGN there are a few concepts to understand:

Graph Construction
^^^^^^^^^^^^^^^^^^

- **Sources**: Sources are the starting point of a task graph. They produce data that can be consumed by
  other tasks.

- **Transforms**: Transforms are tasks that consume data from one or more sources, process it, and produce new data.

- **Sinks**: Sinks are tasks that consume data from one or more sources and do something with it. This could be writing the data to a file, sending it over the network, or anything else.

Control Flow
^^^^^^^^^^^^

Using these concepts, you can create complex task graphs using SGN that process and move data in a variety of ways.
The SGN library provides a simple API for creating and executing task graphs, with a few key types:

- **Frame**: A frame is a unit of data that is passed between tasks in a task graph. Frames can contain any type of data, and can be passed between tasks in a task graph.

- **Pad**: A pad is a connection point between two tasks in a task graph. Pads are used to pass frames between tasks, and can be used to connect tasks in a task graph. An edge is a connection between two pads in a task graph.

- **Element**: An element is a task in a task graph. Elements can be sources, transforms, or sinks, and can be connected together to create a task graph.

- **Pipeline**: A pipeline is a collection of elements that are connected together to form a task graph. Pipelines can be executed to process data, and can be used to create complex data processing workflows.


Guides and Tutorials
--------------------

For more information on how to use SGN, see the following guides and tutorials:

.. toctree::
   :maxdepth: 2
   :caption: Guides and Tutorials:

   concepts


API Reference
-------------

.. toctree::
   :maxdepth: 2
   :caption: API Reference:

   api


