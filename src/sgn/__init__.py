"""Top-level package for sgn. import flattening and version handling
"""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "?.?.?"

# Import flattening
from sgn.apps import Pipeline
from sgn.base import SourcePad, SinkPad, TransformElement, SourceElement, SinkElement
from sgn.frames import Frame, IterFrame
from sgn.sinks import NullSink, CollectSink, DequeSink
from sgn.sources import NullSource, IterSource, DequeSource
from sgn.transforms import CallableTransform
