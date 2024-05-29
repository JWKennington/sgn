from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, ClassVar, Optional
import uuid


@dataclass
class Buffer:
    """
    A generic class to hold the basic unit of data that flows through a graph

    Parameters
    ----------
    EOS : bool, optional
        Whether this buffer indicates end of stream (EOS), default is false
    is_gap : bool, optional
        Whether this buffer is marked as a gap, default is False.
    metadata : dict, optional
        Metadata associated with this buffer.
    """

    EOS: bool = False
    is_gap: bool = False
    metadata: dict = field(default_factory=dict)


@dataclass
class Base:
    """
    A generic class from which all classes that participate in an execution
    graph should be derived.  It enforces a unique name and hashes based on that
    name.

    Parameters
    ----------
    name : str, optional
        The unique name for this object, defaults to the objects unique
        uuid4 hex string if not specified

    """

    name: Optional[str] = None

    def __post_init__(self):
        # give every element a truly unique identifier
        self.__id = uuid.uuid4().hex
        if self.name is None:
            self.name = self.__id

    # Note: we need the Base class to be hashable, so that it can be
    # used as a key in a dictionary, but mutable dataclasses are not
    # hashable by default, so we have to define our own hash function
    # here.
    def __hash__(self) -> int:
        return hash(self.__id)

    def __eq__(self, other) -> bool:
        return hash(self) == hash(other)


@dataclass(eq=False, repr=False)
class Pad(Base):
    """
    Pads are 1:1 with graph nodes but source and sink pads must be grouped into
    elements in order to exchange data from sink->source.  source->sink
    exchanges happen between elements.

    A pad must belong to an element and that element must be provided as a
    keyword argument called "element".  The element must also provide a call
    function that will be executed when the pad is called. The call function
    must take a pad as an argument, e.g., def call(pad):

    Developers should not subclass or use Pad directly. Instead use SourcePad
    or SinkPad.

    Parameters
    ----------
    element : Element, optional
        The Element instance associated with this pad, default None
    call : Callable, optional
        The function that will be called during graph execution for this pad,
        default None

    """

    element: Optional[Element] = None
    call: Optional[Callable] = None

    async def __call__(self) -> None:
        raise NotImplementedError


@dataclass(eq=False, repr=False)
class SourcePad(Pad):
    """
    A pad that provides data through a buffer when called.

    Parameters
    ----------
    outbufs : list[Buffer], optional
        This attribute is populated when the pad is called defined by the
        output of the Pad.call function.
    """

    outbufs: Optional[list[Buffer]] = None

    async def __call__(self) -> None:
        """
        When called, a source pad receives a buffer from the element that the
        pad belongs to.
        """
        self.outbufs = self.call(pad=self)


@dataclass(eq=False, repr=False)
class SinkPad(Pad):
    """
    A pad that receives data from a buffer when asked.  When linked, it returns
    a dictionary suitable for building a graph in graphlib.

    Parameters
    ----------
    other: SourcePad, optional
        This holds the source pad that is linked to this sink pad, default None
    inbufs: List[Buffer], optional
        This holds the buffer provided by the linked source pad. Generally it
        gets set when this SinkPad is called, default None
    """

    other: Optional[SourcePad] = None
    inbufs: Optional[list[Buffer]] = None
    def link(self, other: SourcePad) -> dict[SinkPad, set[SourcePad]]:

        """
        Only sink pads can be linked. A sink pad can be linked to only one
        source pad, but multiple sink pads may link to the same source pad.
        Returns a dictionary of dependencies suitable for adding to a graphlib
        graph.
        """
        assert isinstance(other, SourcePad), "other is not an instanace of SourcePad"
        self.other = other
        return {self: set((other,))}

    async def __call__(self) -> None:
        """
        When called, a sink pad gets the buffer from the linked source pad and
        then calls the element's provided call function.  NOTE: pads must be
        called in the correct order such that the upstream sources have new
        information by the time call is invoked.  This should be done within a
        directed acyclic graph such as those provided by the apps.Pipeline
        class.
        """
        self.inbufs = self.other.outbufs
        self.call(self, self.inbufs)


@dataclass(repr=False)
class Element(Base):
    """
    A basic container to hold source and sink pads. The assmption is that this
    will be a base class for code that actually does something. It should never
    be subclassed directly, instead subclass SourceElement, SinkElement or
    TransformElement

    Parameters
    ----------
    source_pads: list, optional
        The list of SourcePad objects. This must be given for SourceElements or
        TransformElements
    sink_pads: list, optional
        The list of SinkPad objects. This must be given for SinkElements or
        TransformElements
    """

    source_pads: list[SourcePad] = field(default_factory=list)
    sink_pads: list[SinkPad] = field(default_factory=list)
    graph: dict[SourcePad, set[SinkPad]] = field(init=False)

    def __post_init__(self):
        self.graph = {}

    @property
    def source_pad_dict(self) -> dict[str, SourcePad]:
        return {p.name: p for p in self.source_pads}

    @property
    def sink_pad_dict(self) -> dict[str, SinkPad]:
        return {p.name: p for p in self.sink_pads}

    @property
    def pad_list(self) -> list[Pad]:
        return (self.source_pads or []) + (self.sink_pads or [])


@dataclass(repr=False)
class SourceElement(Element):
    """
    Initialize with a list of source pads. Every source pad is added to the
    graph with no dependencies.

    Parameters
    ----------
    source_pad_names : list, optional
        Set the list of source pad names. These need to be unique for an
        element but not for an application. The resulting full names will be
        made with "<self.name>:src:<source_pad_name>"
    """

    source_pad_names: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.source_pads = [
            SourcePad(name="%s:src:%s" % (self.name, n), element=self, call=self.new)
            for n in self.source_pad_names
        ]
        super().__post_init__()
        assert self.source_pads and not self.sink_pads
        self.graph.update({s: set() for s in self.source_pads})

    def new(self, pad: SourcePad) -> list[Buffer]:
        """
        New buffers are created on "pad". Must be provided by subclass
        """
        raise NotImplementedError


@dataclass(repr=False)
class TransformElement(Element):
    """
    Both "source_pads" and "sink_pads" must exist.  All sink pads depend on all
    source pads in a transform element. If you don't want that to be true,
    write more than one transform element.

    Parameters
    ----------
    source_pad_names : list, optional
        Set the list of source pad names. These need to be unique for an
        element but not for an application. The resulting full names will be
        made with "<self.name>:src:<source_pad_name>"
    sink_pad_names : list, optional
        Set the list of sink pad names. These need to be unique for an element
        but not for an application. The resulting full names will be made with
        "<self.name>:sink:<sink_pad_name>"
    """

    source_pad_names: list[str] = field(default_factory=list)
    sink_pad_names: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.source_pads = [
            SourcePad(
                name="%s:src:%s" % (self.name, n),
                element=self,
                call=self.transform,
            )
            for n in self.source_pad_names
        ]
        self.sink_pads = [
            SinkPad(name="%s:sink:%s" % (self.name, n), element=self, call=self.pull)
            for n in self.sink_pad_names
        ]
        super().__post_init__()
        assert self.source_pads and self.sink_pads
        self.graph.update({s: set(self.sink_pads) for s in self.source_pads})

    def pull(self, pad: SourcePad, bufs: list[Buffer]) -> None:
        raise NotImplementedError

    def transform(self, pad: SourcePad) -> list[Buffer]:
        raise NotImplementedError


@dataclass
class SinkElement(Element):
    """
    "sink_pads" must exist but not "source_pads"

    Parameters
    ----------
    sink_pad_names : list, optional
        Set the list of sink pad names. These need to be unique for an element
        but not for an application. The resulting full names will be made with
        "<self.name>:sink:<sink_pad_name>"
    """

    sink_pad_names: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.sink_pads = [
            SinkPad(name="%s:sink:%s" % (self.name, n), element=self, call=self.pull)
            for n in self.sink_pad_names
        ]
        self._at_eos = {p: False for p in self.sink_pads}
        super().__post_init__()
        assert self.sink_pads and not self.source_pads

    @property
    def at_eos(self) -> bool:
        """
        If buffers on any sink pads are End of Stream (EOS), then mark this
        whole element as EOS
        """
        return any(self._at_eos.values())

    def mark_eos(self, pad: SinkPad) -> None:
        """
        Marks a sink pad as receiving the End of Stream (EOS).
        """
        self._at_eos[pad] = True

    def pull(self, pad: SinkPad, bufs: list[Buffer]) -> None:
        raise NotImplementedError
