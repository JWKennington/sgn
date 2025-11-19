"""Base classes for building a graph of elements and pads."""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from .frames import DataSpec, Frame

FrameLike = TypeVar("FrameLike", bound=Frame)

logger = logging.getLogger("sgn")


@dataclass
class UniqueID:
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
class PadLike(ABC):
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

    def __post_init__(self):
        self.pad_name = self.name
        self.name = f"{self.element.name}:{self.pad_type}:{self.pad_name}"

    @abstractmethod
    async def __call__(self) -> None:
        """The call method for a pad must be implemented by the element that the pad
        belongs to.

        This method will be called when the pad is called in the graph.
        """
        ...

    @property
    @abstractmethod
    def pad_type(self) -> str:
        """The pad's type string representation."""
        ...


@dataclass(eq=False, repr=False)
class SourcePad(UniqueID, PadLike):
    """A pad that provides a data Frame when called.

    Args:
        element:
            Element, The Element instance associated with this pad
        call:
            Callable, The function that will be called during graph execution for
            this pad
        name:
            str, optional, The unique name for this object
        output:
            Frame, optional, This attribute is set to be the output Frame when the pad
            is called.
    """

    output: Frame | None = None

    def __post_init__(self):
        PadLike.__post_init__(self)
        UniqueID.__post_init__(self)

    @property
    def pad_type(self) -> str:
        return "src"

    async def __call__(self) -> None:
        """When called, a source pad receives a Frame from the element that the pad
        belongs to."""
        self.output = self.call(pad=self)
        assert isinstance(self.output, Frame)
        if self.element is not None:
            self.element.logger.info("\t%s : %s", self, self.output)


@dataclass(eq=False, repr=False)
class SinkPad(UniqueID, PadLike):
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
        other:
            SourcePad, optional, This holds the source pad that is linked to this sink
            pad, default None
        input:
            Frame, optional, This holds the Frame provided by the linked source pad.
            Generally it gets set when this SinkPad is called, default None
        data_spec:
            DataSpec, optional, This holds a specification for the data stored
            in frames, and is expected to be consistent across frames passing
            through this pad. This is set when this sink pad is first called
    """

    other: SourcePad | None = None
    input: Frame | None = None
    data_spec: DataSpec | None = None

    def __post_init__(self):
        PadLike.__post_init__(self)
        UniqueID.__post_init__(self)

    @property
    def pad_type(self) -> str:
        return "snk"

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
        if self.data_spec is None:
            self.data_spec = self.input.spec
        if not self.data_spec == self.input.spec:
            msg = (
                f"frame received by {self.name} is inconsistent with "
                "previously received frames. previous data specification: "
                f"{self.data_spec}, current data specification: {self.input.spec}"
            )
            raise ValueError(msg)
        self.call(self, self.input)
        if self.element is not None:
            self.element.logger.info("\t%s:%s", self, self.input)


@dataclass(eq=False, repr=False)
class InternalPad(UniqueID, PadLike):
    """A pad that sits inside an element and is called between sink and source pads.
    Internal pads are connected in the elements internal graph according to the below
    (data flows top to bottom)

    snk1   ...  snkN     (if exist)
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

    def __post_init__(self):
        PadLike.__post_init__(self)
        UniqueID.__post_init__(self)

    @property
    def pad_type(self) -> str:
        return "inl"

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
        self.internal_pad = InternalPad(name="inl", element=self, call=self.internal)

    @property
    def source_pad_dict(self) -> dict[str, SourcePad]:
        """Return a dictionary of source pads with the pad name as the key."""
        return {p.name: p for p in self.source_pads}

    @property
    def sink_pad_dict(self) -> dict[str, SinkPad]:
        """Return a dictionary of sink pads with the pad name as the key."""
        return {p.name: p for p in self.sink_pads}

    @property
    def pad_list(self) -> Sequence[Pad]:
        """Return a list of all pads."""
        all_pads: list[Pad] = []
        all_pads.extend(self.source_pads)
        all_pads.extend(self.sink_pads)
        all_pads.append(self.internal_pad)
        return all_pads

    @property
    def logger(self) -> logging.Logger:
        """Return the logger scoped to this element, e.g. sgn.{name}."""
        return logger.getChild(self.name)

    def internal(self) -> None:
        """An optional method to call inbetween sink and source pads of an element, by
        default do nothing."""
        pass


@dataclass(repr=False, kw_only=True)
class SourceElement(ABC, ElementLike):
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

    source_pad_names: Sequence[str] = field(default_factory=list)

    def __post_init__(self):
        """Establish the source pads and graph attributes."""
        super().__post_init__()
        self.source_pads = [
            SourcePad(
                name=pad_name,
                element=self,
                call=self.new,
            )
            for pad_name in self.source_pad_names
        ]
        # short names for easier recall
        self.srcs = {n: p for n, p in zip(self.source_pad_names, self.source_pads)}
        self.rsrcs = {p: n for n, p in zip(self.source_pad_names, self.source_pads)}
        assert self.source_pads, "SourceElement must specify source pads"
        assert not self.sink_pads, "SourceElement must not specify sink pads"
        self.graph.update({s: {self.internal_pad} for s in self.source_pads})

    @abstractmethod
    def new(self, pad: SourcePad) -> Frame:
        """New frames are created on "pad". Must be provided by subclass.

        Args:
            pad:
                SourcePad, The source pad through which the frame is passed

        Returns:
            Frame, The new frame to be passed through the source pad
        """
        ...


@dataclass(repr=False, kw_only=True)
class TransformElement(ABC, ElementLike, Generic[FrameLike]):
    """Both "source_pads" and "sink_pads" must exist. The execution scheduling
    flow of the logic within a TransformElement is as follows: 1.) all sink
    pads, 2.) the internal pad, 3.) all source pads. The execution of all
    downstream logic will be blocked until logic in all upstream pads within
    the same TransformElement has exited.

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
            be made with "<self.name>:snk:<sink_pad_name>"
    """

    source_pad_names: Sequence[str] = field(default_factory=list)
    sink_pad_names: Sequence[str] = field(default_factory=list)

    def __post_init__(self):
        """Establish the source pads and sink pads and graph attributes."""
        super().__post_init__()
        self.source_pads = [
            SourcePad(
                name=pad_name,
                element=self,
                call=self.new,
            )
            for pad_name in self.source_pad_names
        ]
        self.sink_pads = [
            SinkPad(
                name=pad_name,
                element=self,
                call=self.pull,
            )
            for pad_name in self.sink_pad_names
        ]
        # short names for easier recall
        self.srcs = {n: p for n, p in zip(self.source_pad_names, self.source_pads)}
        self.snks = {n: p for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self.rsrcs = {p: n for n, p in zip(self.source_pad_names, self.source_pads)}
        self.rsnks = {p: n for n, p in zip(self.sink_pad_names, self.sink_pads)}
        assert (
            self.source_pads and self.sink_pads
        ), "TransformElement must specify both sink and source pads"

        # Make maximal bipartite graph in two pieces
        # First, (all sinks -> internal)
        self.graph.update({self.internal_pad: set(self.sink_pads)})
        # Second, (internal -> all sources)
        self.graph.update({s: {self.internal_pad} for s in self.source_pads})

    @abstractmethod
    def pull(self, pad: SinkPad, frame: FrameLike) -> None:
        """Pull data from the input pads (source pads of upstream elements), must be
        implemented by subclasses.

        Args:
            pad:
                SinkPad, The sink pad that is receiving the frame
            frame:
                Frame, The frame that is pulled from the source pad
        """
        ...

    @abstractmethod
    def new(self, pad: SourcePad) -> FrameLike:
        """New frames are created on "pad". Must be provided by subclass.

        Args:
            pad:
                SourcePad, The source pad through which the frame is passed

        Returns:
            Frame, The new frame to be passed through the source pad
        """
        ...


@dataclass(kw_only=True)
class SinkElement(ABC, ElementLike, Generic[FrameLike]):
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

    sink_pad_names: Sequence[str] = field(default_factory=list)

    def __post_init__(self):
        """Establish the sink pads and graph attributes."""
        super().__post_init__()
        self.sink_pads = [
            SinkPad(
                name=pad_name,
                element=self,
                call=self.pull,
            )
            for pad_name in self.sink_pad_names
        ]
        # short names for easier recall
        self.snks = {n: p for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self.rsnks = {p: n for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self._at_eos = {p: False for p in self.sink_pads}
        assert self.sink_pads, "SinkElement must specify sink pads"
        assert not self.source_pads, "SinkElement must not specify any source pads"
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

    @abstractmethod
    def pull(self, pad: SinkPad, frame: FrameLike) -> None:
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
        ...


Element = TransformElement | SinkElement | SourceElement
Pad = SinkPad | SourcePad | InternalPad
