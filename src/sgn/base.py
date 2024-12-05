"""Base classes for building a graph of elements and pads."""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Union

SGN_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
SGN_LOG_LEVEL_VAR = "SGNLOGLEVEL"


def get_sgn_logger(name: str, levels: Dict[str, int]) -> logging.Logger:
    """Utility function for constructing a logger with a given name and log level.

    Args:
        name:
            str, The name of the logger
        levels:
            Dict[str, int], A dictionary of log levels to choose from

    Returns:
        logging.Logger, The logger with the specified name and log level
    """
    sgnlogger = logging.getLogger(name)
    sgnlogger.addHandler(logging.StreamHandler())

    def parse_elem_level(x, default=name):
        y = x.split(":")
        name, level = (default, y[0]) if len(y) == 1 else (y[0], y[1])
        if level not in levels:
            raise ValueError(f"Invalid log level: {level}, choose " f"{list(levels)}")
        return name, level

    if SGN_LOG_LEVEL_VAR in os.environ:
        levels = dict(
            [parse_elem_level(x) for x in os.environ[SGN_LOG_LEVEL_VAR].split()]
        )
        for n in levels:
            if n == name:
                sgnlogger.setLevel(levels[n])
            else:
                sgnlogger.getChild(n).setLevel(levels[n])
    return sgnlogger


LOGGER = get_sgn_logger("sgn", SGN_LOG_LEVELS)


@dataclass
class Frame:
    """Generic class to hold the basic unit of data that flows through a graph.

    Args:
        EOS:
            bool, default False, Whether this frame indicates end of stream (EOS)
        is_gap:
            bool, default False, Whether this frame is marked as a gap
        metadata:
            dict, optional, Metadata associated with this frame.
    """

    EOS: bool = False
    is_gap: bool = False
    metadata: dict = field(default_factory=dict)
    data: Any = None

    def __post_init__(self):
        pass


class _PostInitBase:
    """Mixin class used to resolve issues with the builtin object class with
    __post_init__ and using multiple inheritance.

    See https://stackoverflow.com/a/59987363/23231262.
    """

    def __post_init__(self):
        """Intercept the __post_init__ calls so they aren't relayed to `object`"""
        pass


@dataclass
class UniqueID(_PostInitBase):
    """Generic class from which all classes that participate in an execution graph
    should be derived. Enforces a unique name and hashes based on that name.

    Args:
        name:
            str, optional, The unique name for this object, defaults to the objects
            unique uuid4 hex string if not specified
    """

    name: str = ""
    _id: str = field(init=False)

    def __post_init__(self):
        """Handle setup of the UniqueID class, including the `._id` attribute."""
        super().__post_init__()
        # give every element a truly unique identifier
        self._id = uuid.uuid4().hex
        if not self.name:
            self.name = self._id

    def __hash__(self) -> int:
        """Compute the hash of the object based on the unique id.

        Notes:
            Motivation:
                we need the Base class to be hashable, so that it can be
                used as a key in a dictionary, but mutable dataclasses are not
                hashable by default, so we have to define our own hash function
                here.
            Stability:
                As currently implemented, the hash of a UniqueID object will not be
                stable across python sessions, and should therefore not be used for
                checksum purposes.

        Returns:
            int, hash of the object
        """
        return hash(self._id)

    def __eq__(self, other) -> bool:
        """Check if two objects are equal based on their unique id and types."""
        return hash(self) == hash(other)


@dataclass(eq=False, repr=False)
class PadLike:
    """Pads are 1:1 with graph nodes but source and sink pads must be grouped into
    elements in order to exchange data from sink->source.  source->sink exchanges happen
    between elements.

    A pad must belong to an element and that element must be provided as a
    keyword argument called "element".  The element must also provide a call
    function that will be executed when the pad is called. The call function
    must take a pad as an argument, e.g., def call(pad):

    Developers should not subclass or use Pad directly. Instead use SourcePad
    or SinkPad.

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for
            this pad
    """

    element: Element
    call: Callable
    is_linked: bool = False

    async def __call__(self) -> None:
        """The call method for a pad must be implemented by the element that the pad
        belongs to.

        This method will be called when the pad is called in the graph.
        """
        raise NotImplementedError


@dataclass(eq=False, repr=False)
class _SourcePadLike(PadLike):
    """A pad that provides a data Frame when called.

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for
            this pad
        output:
            Frame, optional, This attribute is set to be the output Frame when the pad
            is called.
    """

    output: Optional[Frame] = None


@dataclass(eq=False, repr=False)
class _SinkPadLike(PadLike):
    """A pad that receives a data Frame when called.  When linked, it returns a
    dictionary suitable for building a graph in graphlib.

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for
            this pad
        other:
            SourcePad, optional, This holds the source pad that is linked to this sink
            pad, default None
        input:
            Frame, optional, This holds the Frame provided by the linked source pad.
            Generally it gets set when this SinkPad is called, default None
    """

    other: Optional[SourcePad] = None
    input: Optional[Frame] = None


@dataclass(eq=False, repr=False)
class _InternalPadLike(PadLike):
    """A pad that sits inside an element and is called between sink and source pads.
    Internal pads are connected in the elements internal graph according to the below
    (data flows top to bottom)

    sink1  ...  sinkN     (if exist)
      \\   ...   //
         internal      (always exists)
      //   ...   \\
     src1  ...  srcM     (if exist)

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for
            this pad
    """


@dataclass(eq=False, repr=False)
class SourcePad(UniqueID, _SourcePadLike):
    """A pad that provides a data Frame when called.

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for
            this pad
        name:
            str, optional, The unique name for this object
    """

    async def __call__(self) -> None:
        """When called, a source pad receives a Frame from the element that the pad
        belongs to."""
        self.output = self.call(pad=self)
        assert isinstance(self.output, Frame)
        if self.element is not None:
            LOGGER.getChild(self.element.name).info("\t%s : %s", self, self.output)


@dataclass(eq=False, repr=False)
class SinkPad(UniqueID, _SinkPadLike):
    """A pad that receives a data Frame when called.  When linked, it returns a
    dictionary suitable for building a graph in graphlib.

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for this
            pad, takes two arguments, the pad and the frame
        name:
            str, optional, The unique name for this object
    """

    def link(self, other: SourcePad) -> dict[Pad, set[Pad]]:
        """Returns a dictionary of dependencies suitable for adding to a graphlib graph.

        Args:
            other:
                SourcePad, The source pad to link to this sink pad

        Notes:
            Many-to-one (source, sink) Not Supported:
                Only sink pads can be linked. A sink pad can be linked to only one
                source pad, but multiple sink pads may link to the same source pad.

        Returns:
            dict[SinkPad, set[SourcePad]], a dictionary of dependencies suitable for
            adding to a graphlib graph
        """
        assert isinstance(other, SourcePad), "other is not an instance of SourcePad"
        self.other = other
        self.is_linked = True
        other.is_linked = True
        return {self: {other}}

    async def __call__(self) -> None:
        """When called, a sink pad gets a Frame from the linked source pad and then
        calls the element's provided call function.

        Notes:
            Pad Call Order:
                pads must be called in the correct order such that the upstream sources
                have new information by the time call is invoked. This should be done
                within a directed acyclic graph such as those provided by the
                apps.Pipeline class.
        """
        assert isinstance(self.other, SourcePad), "Sink pad has not been linked"
        self.input = self.other.output
        assert isinstance(self.input, Frame)
        self.call(self, self.input)
        if self.element is not None:
            LOGGER.getChild(self.element.name).info("\t%s:%s", self, self.input)


@dataclass(eq=False, repr=False)
class InternalPad(UniqueID, _InternalPadLike):
    """A pad that sits inside an element and is called between sink and source pads.
    Internal pads are connected in the elements internal graph according to the below
    (data flows top to bottom)

    sink1  ...  sinkN     (if exist)
      \\   ...   //
         internal      (always exists)
      //   ...   \\
     src1  ...  srcM     (if exist)

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for
            this pad
        name:
            str, optional, The unique name for this object
    """

    async def __call__(self) -> None:
        """When called, an internal pad receives a Frame from the element that the pad
        belongs to."""
        self.call()


@dataclass(repr=False)
class ElementLike(UniqueID):
    """A basic container to hold source and sink pads. The assumption is that this will
    be a base class for code that actually does something. It should never be subclassed
    directly, instead subclass SourceElement, SinkElement or TransformElement.

    Args:
        source_pads:
            list, optional, The list of SourcePad objects. This must be given for
            SourceElements or TransformElements
        sink_pads:
            list, optional, The list of SinkPad objects. This must be given for
            SinkElements or TransformElements
    """

    source_pads: list[SourcePad] = field(default_factory=list)
    sink_pads: list[SinkPad] = field(default_factory=list)
    internal_pad: InternalPad = field(init=False)
    graph: dict[Pad, set[Pad]] = field(init=False)

    def __post_init__(self):
        """Establish the graph attribute as an empty dictionary."""
        super().__post_init__()
        self.graph = {}
        self.internal_pad = InternalPad(
            name=f"{self.name}:inl:inl", element=self, call=self.internal
        )

    @property
    def source_pad_dict(self) -> dict[str, SourcePad]:
        """Return a dictionary of source pads with the pad name as the key."""
        return {p.name: p for p in self.source_pads}

    @property
    def sink_pad_dict(self) -> dict[str, SinkPad]:
        """Return a dictionary of sink pads with the pad name as the key."""
        return {p.name: p for p in self.sink_pads}

    @property
    def pad_list(self) -> Sequence[Union[Pad, None]]:
        """Return a list of all pads."""
        all_pads: list[Union[Pad, None]] = []
        all_pads.extend(self.source_pads)
        all_pads.extend(self.sink_pads)
        all_pads.append(self.internal_pad)
        return all_pads

    def internal(self) -> None:
        """An optional method to call inbetween sink and source pads of an element, by
        default do nothing."""
        pass


@dataclass(repr=False)
class SourceElement(ElementLike):
    """Initialize with a list of source pads. Every source pad is added to the graph
    with no dependencies.

    Args:
        name:
            str, optional, The unique name for this object
        source_pad_names:
            list, optional, Set the list of source pad names. These need to be unique
            for an element but not for an application. The resulting full names will be
            made with "<self.name>:src:<source_pad_name>"
    """

    source_pad_names: Iterable[str] = field(default_factory=list)

    def __post_init__(self):
        """Establish the source pads and graph attributes."""
        super().__post_init__()
        self.source_pads = [
            SourcePad(
                name=f"{self.name}:src:{n}",
                element=self,
                call=self.new,
            )
            for n in self.source_pad_names
        ]
        # short names for easier recall
        self.srcs = {n: p for n, p in zip(self.source_pad_names, self.source_pads)}
        self.rsrcs = {p: n for n, p in zip(self.source_pad_names, self.source_pads)}
        assert self.source_pads and not self.sink_pads
        self.graph.update({s: {self.internal_pad} for s in self.source_pads})

    def new(self, pad: SourcePad) -> Frame:
        """New frames are created on "pad". Must be provided by subclass.

        Args:
            pad:
                SourcePad, The source pad through which the frame is passed

        Returns:
            Frame, The new frame to be passed through the source pad
        """
        raise NotImplementedError


@dataclass(repr=False)
class TransformElement(ElementLike):
    """Both "source_pads" and "sink_pads" must exist.  All sink pads depend on all
    source pads in a transform element. If you don't want that to be true, write more
    than one transform element.

    Args:
        name:
            str, optional, The unique name for this object
        source_pad_names:
            list, optional, Set the list of source pad names. These need to be unique
            for an element but not for an application. The resulting full names will
            be made with "<self.name>:src:<source_pad_name>"
        sink_pad_names:
            list, optional, Set the list of sink pad names. These need to be unique
            for an element but not for an application. The resulting full names will
            be made with "<self.name>:sink:<sink_pad_name>"
    """

    source_pad_names: Iterable[str] = field(default_factory=list)
    sink_pad_names: Iterable[str] = field(default_factory=list)

    def __post_init__(self):
        """Establish the source pads and sink pads and graph attributes."""
        super().__post_init__()
        self.source_pads = [
            SourcePad(
                name=f"{self.name}:src:{n}",
                element=self,
                call=self.transform,
            )
            for n in self.source_pad_names
        ]
        self.sink_pads = [
            SinkPad(
                name=f"{self.name}:sink:{n}",
                element=self,
                call=self.pull,
            )
            for n in self.sink_pad_names
        ]
        # short names for easier recall
        self.srcs = {n: p for n, p in zip(self.source_pad_names, self.source_pads)}
        self.snks = {n: p for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self.rsrcs = {p: n for n, p in zip(self.source_pad_names, self.source_pads)}
        self.rsnks = {p: n for n, p in zip(self.sink_pad_names, self.sink_pads)}
        assert self.source_pads and self.sink_pads

        # Make maximal bipartite graph in two pieces
        # First, (all sinks -> internal)
        self.graph.update({self.internal_pad: set(self.sink_pads)})
        # Second, (internal -> all sources)
        self.graph.update({s: {self.internal_pad} for s in self.source_pads})

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Pull data from the input pads (source pads of upstream elements), must be
        implemented by subclasses.

        Args:
            pad:
                SinkPad, The sink pad that is receiving the frame
            frame:
                Frame, The frame that is pulled from the source pad
        """
        raise NotImplementedError

    def transform(self, pad: SourcePad) -> Frame:
        """The transform function must be provided by the subclass. It should take the
        source pad as an argument and return a new frame.

        Args:
            pad:
                SourcePad, The source pad that is providing the frame to be transformed

        Returns:
            Frame, The transformed frame
        """
        raise NotImplementedError


@dataclass
class SinkElement(ElementLike):
    """Sink element represents a terminal node in a pipeline, that typically writes data
    to disk, etc. Sink_pads must exist but not source_pads.

    Args:
        name:
            str, optional, The unique name for this object
        sink_pad_names:
            list, optional, Set the list of sink pad names. These need to be unique for
            an element but not for an application. The resulting full names will be
            made with "<self.name>:sink:<sink_pad_name>"
    """

    sink_pad_names: Iterable[str] = field(default_factory=list)

    def __post_init__(self):
        """Establish the sink pads and graph attributes."""
        super().__post_init__()
        self.sink_pads = [
            SinkPad(
                name=f"{self.name}:sink:{n}",
                element=self,
                call=self.pull,
            )
            for n in self.sink_pad_names
        ]
        # short names for easier recall
        self.snks = {n: p for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self.rsnks = {p: n for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self._at_eos = {p: False for p in self.sink_pads}
        assert self.sink_pads and not self.source_pads
        self.sink_pad_names_full = [p.name for p in self.sink_pads]

        # Update graph to be (all sinks -> internal)
        self.graph.update({self.internal_pad: set(self.sink_pads)})

    @property
    def at_eos(self) -> bool:
        """If frames on any sink pads are End of Stream (EOS), then mark this whole
        element as EOS.

        Returns:
            bool, True if any sink pad is at EOS, False otherwise
        """
        # TODO generalize this to be able to choose any v. all EOS propagation
        return any(self._at_eos.values())

    def mark_eos(self, pad: SinkPad) -> None:
        """Marks a sink pad as receiving the End of Stream (EOS). The EOS marker signals
        that no more frames will be received on this pad.

        Args:
            pad:
                SinkPad, The sink pad that is receiving the
        """
        self._at_eos[pad] = True

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Pull for a SinkElement represents the action of associating a frame with a
        particular input source pad a frame. This function must be provided by the
        subclass, and is where any "final" behavior must occur, e.g. writing to disk,
        etc.

        Args:
            pad:
                SinkPad, The sink pad that is receiving the frame
            frame:
                Frame, The frame that is being received
        """
        raise NotImplementedError


Element = Union[TransformElement, SinkElement, SourceElement, ElementLike]
Pad = Union[SinkPad, SourcePad, InternalPad]
