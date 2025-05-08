"""Frame classes for the SGN framework."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Iterable


@dataclass(frozen=True, eq=True)
class DataSpec:
    """A specification for the type of data stored in frames.

    All properties in this specification will be expected to match what is
    stored in the frame, and what is being transferred between source and sink
    pads.

    """

    def update(self, **kwargs) -> DataSpec:
        return replace(self, **kwargs)


@dataclass
class Frame:
    """Generic class to hold the basic unit of data that flows through a graph.

    Args:
        EOS:
            bool, default False, Whether this frame indicates end of stream (EOS)
        is_gap:
            bool, default False, Whether this frame is marked as a gap
        spec:
            DataSpec, optional, a specification for the data captured in this frame
        data:
            Any, the data to store in the frame
        metadata:
            dict, optional, Metadata associated with this frame.
    """

    EOS: bool = False
    is_gap: bool = False
    spec: DataSpec = field(default_factory=DataSpec)
    data: Any = None
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        pass


@dataclass
class IterFrame(Frame):
    """A frame whose data attribute is an iterable.

    Args:
        data:
            Iterable, the data to store in the frame
    """

    data: Iterable[Any] = field(default_factory=list)
