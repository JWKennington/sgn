"""Sink elements for the SGN framework."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Dict, MutableSequence

from sgn.base import Frame, SinkElement, SinkPad


@dataclass
class NullSink(SinkElement):
    """A sink that does precisely nothing.

    It is useful for testing and debugging, or for pipelines that do
    not need a sink, but require one to be present in the pipeline.

    Args:
        verbose:
            bool, print frames as they pass through the internal pad

    """

    verbose: bool = False

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Do nothing on pull.

        Args:
            pad:
                SinkPad, the pad that the frame is pulled into
            frame:
                Frame, the frame that is pulled into the sink
        """
        if frame.EOS:
            self.mark_eos(pad)
        if self.verbose is True:
            print(frame)


@dataclass
class CollectSink(SinkElement):
    """A sink element that has one collection per sink pad.

    Each frame that is pulled into the sink is added to the collection for that
    pad using a ".append" method. If the extract_data flag is set, the data is
    extracted from the frame and added to the deque, otherwise the frame
    itself is added to the collection.

    Args:
        collects:
            dict[str, Collection], a mapping of sink pads to Collections, where the key
            is the pad name and the value is the Collection. The Collection must have an
            append method.
        extract_data:
            bool, default True, flag to indicate if the data should be extracted from
            the frame before adding it to the deque

    Notes:
        Ignoring empty frames:
            If the frame is empty, it is not added to the deque. The motivating
            principle is that "empty frames preserve the sink deque". An empty deque
            is equivalent (for our purposes) to a deque filled with "None" values,
            so we prevent the latter from being possible.
    """

    collects: Dict[str, MutableSequence] = field(default_factory=dict)
    extract_data: bool = True
    collection_factory: Callable = list

    def __post_init__(self):
        """Post init checks for the DequeSink element."""
        super().__post_init__()
        # Setup the deque_map if not given
        if not self.collects:
            self.collects = {
                pad.name: self.collection_factory() for pad in self.sink_pads
            }
        else:
            self.collects = {
                name: self.collection_factory(iterable)
                for name, iterable in self.collects.items()
            }

        # Check that the deque_map has the correct number of deque s
        if not len(self.collects) == len(self.sink_pads):
            raise ValueError("The number of iterables must match the number of pads")

        # Check that the deque_map has the correct pad names
        for pad_name in self.collects:
            if pad_name not in self.sink_pad_names_full:
                raise ValueError(
                    f"DequeSink has a iterable for a pad that does not exist, "
                    f"got: {pad_name}, options are: {self.sink_pad_names}"
                )

        # Create attr for storing most recent inputs per pad
        self.inputs = {}

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Pull a frame into the sink and add it to the deque for that pad.

        Args:
            pad:
                SinkPad, the pad that the frame is pulled into
            frame:
                Frame, the frame that is pulled into the sink
        """
        if frame.EOS:
            self.mark_eos(pad)

        self.inputs[pad.name] = frame

    def internal(self) -> None:
        """Internal action is to append all most recent frames to the associated
        collections, then empty the inputs dict.

        Args:
            pad:

        Returns:
        """
        for pad_name, frame in self.inputs.items():
            if frame.data is not None:
                self.collects[pad_name].append(
                    frame.data if self.extract_data else frame
                )

        self.inputs = {}


@dataclass
class DequeSink(CollectSink):
    """A sink element that has one double-ended-queue (deque) per sink pad.

    Each frame that is pulled into the sink is added to the deque for that pad.
    If the extract_data flag is set, the data is extracted from the frame and
    added to the deque , otherwise the frame itself is added to the deque.

    Args:
        collects:
            dict[str, deque ], a mapping of sink pads to deque s, where the key
            is the pad name and the value is the deque
        extract_data:
            bool, default True, flag to indicate if the data should be extracted from
            the frame before adding it to the deque

    Notes:
        Ignoring empty frames:
            If the frame is empty, it is not added to the deque. The motivating
            principle is that "empty frames preserve the sink deque". An empty deque
            is equivalent (for our purposes) to a deque filled with "None" values,
            so we prevent the latter from being possible.
    """

    collection_factory: Callable = deque

    @property
    def deques(self) -> dict[str, MutableSequence]:
        """Explicit alias for collects.

        Returns:
            dict[str, deque ]: the deques for the sink
        """
        return self.collects
