"""Top-level package for sgn. import flattening and version handling
"""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "?.?.?"

# Import flattening
from sgn.apps import Pipeline
from sgn.base import SinkElement, SinkPad, SourceElement, SourcePad, TransformElement
from sgn.frames import Frame, IterFrame
from sgn.sinks import CollectSink, DequeSink, NullSink
from sgn.sources import DequeSource, IterSource, NullSource
from sgn.transforms import CallableTransform
