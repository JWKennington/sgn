from dataclasses import dataclass
from typing import Callable
import random

@dataclass
class Buffer:
    """
    A generic class to hold the basic unit of data that flows through a graph
    """
    EOS: bool = False
    is_gap: bool = False
    metadata: dict = None

@dataclass
class Base(object):
    """
    A generic class from which all classes that participate in an execution
    graph should be derived.  It enforces a unique name and hashes based on that
    name. 
    """
    # according to the docs, this won't be initialized as an instance variable
    # since it is missing the type hint
    registry = {}
    name: str = str(random.getrandbits(64))

    def __post_init__(self):
        assert self.name not in Base.registry
        Base.registry[self.name] = self

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __repr__(self):
        return self.name

@dataclass(repr=False)
class Element(Base):
    """
    A basic container to hold src and sink pads. The assmption is that this
    will be a base class for code that actually does something. It should never be
    subclassed directly, instead subclass SrcElement, SinkElement or
    TransformElement
    """
    graph: dict = None
    source_pads: list = None
    sink_pads: list = None
    link_map: dict = None

    def __post_init__(self):
        if self.graph is None:
            self.graph = {}
        if self.link_map is None:
            self.link_map = {}
        for sink_pad, src_pad in self.link_map.items():
            if sink_pad not in self.sink_pad_dict:
                raise ValueError("%s not in %s's list of sink pads %s" % (sink_pad, self.name, list(self.sink_pad_dict.keys())))

    @property
    def src_pad_dict(self):
        return {p.name:p for p in self.src_pads}

    @property
    def sink_pad_dict(self):
        return {p.name:p for p in self.sink_pads}

@dataclass(eq=False, repr=False)
class Pad(Base):
    """
    Pads are 1:1 with graph nodes but src and sink pads must be grouped into
    elements in order to exchange data from sink->src.  src->sink exchanges happen
    between elements.

    A pad must belong to an element and that element must be provided as a
    keyword argument called "element".  The element must also provide a call
    function that will be executed when the pad is called. The call function must
    take a pad as an argument, e.g., def call(pad):
    """
    element: Element = None
    call: Callable = None

@dataclass(eq=False, repr=False)
class SrcPad(Pad):
    """
    A pad that provides data through a buffer when asked
    """
    outbuf: Buffer = None

    async def __call__(self):
        """
	When called, a source pad receives a buffer from the element that the
        pad belongs to.
        """
        self.outbuf = self.call(pad = self)


@dataclass(eq=False, repr=False)
class SinkPad(Pad):
    """
    A pad that receives data from a buffer when asked.  When linked, it returns
    a dictionary suitable for building a graph in graphlib.
    """
    other: Pad = None
    inbuf: Buffer = None

    def link(self, other):
        """
	Only sink pads can be linked. A sink pad can be linked to only one
	source pad, but multiple sink pads may link to the same src pad.
        Returns a dictionary of dependencies suitable for adding to a graphlib graph.
        """
        self.other = other
        assert isinstance(self.other, SrcPad)
        return {self: set((other,))}
    async def __call__(self):
        """
	When called, a sink pad gets the buffer from the linked source pad and
        then calls the element's provided call function.  NOTE: pads must be called in
        the correct order such that the upstream sources have new information by the
        time call is invoked.  This should be done within a directed acyclic graph such
        as those provided by the apps.Pipeline class.
        """
        self.inbuf = self.other.outbuf
        self.call(self, self.inbuf)


@dataclass(repr=False)
class SrcElement(Element):
    """
    Initialize with a list of source pads. Every source pad is added to the graph with no dependencies.
    """

    def __post_init__(self):
        super().__post_init__()
        assert self.src_pads is not None
        self.graph.update({s: set() for s in self.src_pads})

@dataclass(repr=False)
class TransformElement(Element):
    """
    Both "src_pads" and "sink_pads" must be in kwargs.  All sink pads
    depend on all source pads in a transform element. If you don't want that to be
    true, write more than one transform element.
    """

    def __post_init__(self):
        super().__post_init__()
        assert self.src_pads is not None and self.sink_pads is not None
        self.graph.update({s: set(self.sink_pads) for s in self.src_pads})
        
@dataclass(repr=False)
class SinkElement(Element):
    """
    "sink_pads" must be in kwargs
    """

    def __post_init__(self):
        super().__post_init__()
        assert self.sink_pads is not None
