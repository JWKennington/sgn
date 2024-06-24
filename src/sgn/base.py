from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional, Union


@dataclass
class Frame:
    """
    A generic class to hold the basic unit of data that flows through a graph

    Parameters
    ----------
    EOS : bool, optional
        Whether this frame indicates end of stream (EOS), default is false
    is_gap : bool, optional
        Whether this frame is marked as a gap, default is False.
    metadata : dict, optional
        Metadata associated with this frame.

    """

    EOS: bool = False
    is_gap: bool = False
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        self.metadata["__graph__"] = ""


class _PostInitBase:
    """
    Used to resolve issues with the builtin object class with __post_init__
    and using multiple inheritance. See https://stackoverflow.com/a/59987363/23231262.
    """

    def __post_init__(self):
        # just intercept the __post_init__ calls so they
        # aren't relayed to `object`
        pass


@dataclass
class UniqueID(_PostInitBase):
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

    name: str = ""
    _id: str = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        # give every element a truly unique identifier
        self._id = uuid.uuid4().hex
        if not self.name:
            self.name = self._id

    # Note: we need the Base class to be hashable, so that it can be
    # used as a key in a dictionary, but mutable dataclasses are not
    # hashable by default, so we have to define our own hash function
    # here.
    def __hash__(self) -> int:
        return hash(self._id)

    def __eq__(self, other) -> bool:
        return hash(self) == hash(other)


@dataclass(eq=False, repr=False)
class PadLike:
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
    element : Element
        The Element instance associated with this pad
    call : Callable
        The function that will be called during graph execution for this pad

    """

    element: Element
    call: Callable

    async def __call__(self) -> None:
        raise NotImplementedError


@dataclass(eq=False, repr=False)
class _SourcePadLike(PadLike):
    """
    A pad that provides a data Frame when called.

    Parameters
    ----------
    output : Frame, optional
        This attribute is set to be the output Frame when the pad is called.

    """

    output: Optional[Frame] = None


@dataclass(eq=False, repr=False)
class _SinkPadLike(PadLike):
    """
    A pad that receives a data Frame when called.  When linked, it returns
    a dictionary suitable for building a graph in graphlib.

    Parameters
    ----------
    other: SourcePad, optional
        This holds the source pad that is linked to this sink pad, default None
    input: Frame, optional
        This holds the Frame provided by the linked source pad. Generally it
        gets set when this SinkPad is called, default None
    """

    other: Optional[SourcePad] = None
    input: Optional[Frame] = None


@dataclass(eq=False, repr=False)
class SourcePad(UniqueID, _SourcePadLike):
    """
    A pad that provides a data Frame when called.

    Parameters
    ----------
    output : Frame, optional
        This attribute is set to be the output Frame when the pad is called.

    """

    async def __call__(self) -> None:
        """
        When called, a source pad receives a Frame from the element that
        the pad belongs to.

        """
        self.output = self.call(pad=self)
        assert isinstance(self.output, Frame)
        self.output.metadata["__graph__"] += "-> %s " % self.name


@dataclass(eq=False, repr=False)
class SinkPad(UniqueID, _SinkPadLike):
    """
    A pad that receives a data Frame when called.  When linked, it returns
    a dictionary suitable for building a graph in graphlib.

    Parameters
    ----------
    other: SourcePad, optional
        This holds the source pad that is linked to this sink pad, default None
    input: Frame, optional
        This holds the Frame provided by the linked source pad. Generally it
        gets set when this SinkPad is called, default None
    """

    def link(self, other: SourcePad) -> dict[SinkPad, set[SourcePad]]:
        """
        Only sink pads can be linked. A sink pad can be linked to only one
        source pad, but multiple sink pads may link to the same source pad.
        Returns a dictionary of dependencies suitable for adding to a graphlib
        graph.
        """
        assert isinstance(other, SourcePad), "other is not an instance of SourcePad"
        self.other = other
        return {self: set((other,))}

    async def __call__(self) -> None:
        """
        When called, a sink pad gets a Frame from the linked source pad and
        then calls the element's provided call function.  NOTE: pads must be
        called in the correct order such that the upstream sources have new
        information by the time call is invoked.  This should be done within a
        directed acyclic graph such as those provided by the apps.Pipeline
        class.

        """
        assert isinstance(self.other, SourcePad), "Sink pad has not been linked"
        self.input = self.other.output
        assert isinstance(self.input, Frame)
        self.input.metadata["__graph__"] += "-> %s " % self.name
        self.call(self, self.input)


@dataclass(repr=False)
class ElementLike(_PostInitBase):
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
        super().__post_init__()
        self.graph = {}

    @property
    def source_pad_dict(self) -> dict[str, SourcePad]:
        return {p.name: p for p in self.source_pads}

    @property
    def sink_pad_dict(self) -> dict[str, SinkPad]:
        return {p.name: p for p in self.sink_pads}

    @property
    def pad_list(self) -> list[Pad]:
        return self.source_pads + self.sink_pads


@dataclass(repr=False)
class SourceElement(UniqueID, ElementLike):
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
        super().__post_init__()
        self.source_pads = [
            SourcePad(name="%s:src:%s" % (self.name, n), element=self, call=self.new)
            for n in self.source_pad_names
        ]
        assert self.source_pads and not self.sink_pads
        self.graph.update({s: set() for s in self.source_pads})

    def new(self, pad: SourcePad) -> Frame:
        """
        New frames are created on "pad". Must be provided by subclass
        """
        raise NotImplementedError


@dataclass(repr=False)
class TransformElement(UniqueID, ElementLike):
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
        super().__post_init__()
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
        assert self.source_pads and self.sink_pads
        self.graph.update({s: set(self.sink_pads) for s in self.source_pads})

    def pull(self, pad: SourcePad, frame: Frame) -> None:
        raise NotImplementedError

    def transform(self, pad: SourcePad) -> Frame:
        raise NotImplementedError


@dataclass
class SinkElement(UniqueID, ElementLike):
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
        super().__post_init__()
        self.sink_pads = [
            SinkPad(name="%s:sink:%s" % (self.name, n), element=self, call=self.pull)
            for n in self.sink_pad_names
        ]
        self._at_eos = {p: False for p in self.sink_pads}
        assert self.sink_pads and not self.source_pads

    @property
    def at_eos(self) -> bool:
        """
        If frames on any sink pads are End of Stream (EOS), then mark this
        whole element as EOS
        """
        return any(self._at_eos.values())

    def mark_eos(self, pad: SinkPad) -> None:
        """
        Marks a sink pad as receiving the End of Stream (EOS).
        """
        self._at_eos[pad] = True

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        raise NotImplementedError


Element = Union[TransformElement, SinkElement, SourceElement]
Pad = Union[SinkPad, SourcePad]
