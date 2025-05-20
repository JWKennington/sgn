"""Top-level package for sgn.

import flattening and version handling
"""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "0.0.0"

# Import flattening
from sgn.base import SourcePad, SinkPad, TransformElement, SourceElement, SinkElement
from sgn.frames import Frame, IterFrame
from sgn.sinks import CollectSink, DequeSink, NullSink
from sgn.sources import DequeSource, IterSource, NullSource, SignalEOS
from sgn.transforms import CallableTransform
from sgn.apps import Pipeline
