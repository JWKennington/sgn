from functools import wraps
import inspect

# These could be moved out someday to other modules

# From https://stackoverflow.com/questions/1389180/automatically-initialize-instance-variables
def initializer(func):
    """
    Automatically assigns the parameters.

    >>> class process:
    ...     @initializer
    ...     def __init__(self, cmd, reachable=False, user='root'):
    ...         pass
    >>> p = process('halt', True)
    >>> p.cmd, p.reachable, p.user
    ('halt', True, 'root')
    """
    names, varargs, keywords, defaults = inspect.getargspec(func)

    @wraps(func)
    def wrapper(self, *args, **kargs):
        for name, arg in list(zip(names[1:], args)) + list(kargs.items()):
            setattr(self, name, arg)

        for name, default in zip(reversed(names), reversed(defaults or [])):
            if not hasattr(self, name):
                setattr(self, name, default)

        func(self, *args, **kargs)

    return wrapper

class Buffer(object):
    """
    A generic class to hold the basic unit of data that flows through a graph
    """
    @initializer
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def __repr__(self):
        return str(self.kwargs)

class Base(object):
    """
    A generic class from which all classes that participate in an execution
    graph should be derived.  It enforces a unique name and hashes based on that
    name. 
    """
    registry = set()

    @initializer
    def __init__(self, **kwargs):
        assert "name" in kwargs
        assert kwargs["name"] not in Base.registry
        Base.registry.add(kwargs["name"])

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __repr__(self):
        return self.name

class Pad(Base):
    """
    Pads are 1:1 with graph nodes but src and sink pads may be grouped into elements.
    """
    @initializer
    def __init__(self, **kwargs):
        assert "element" in kwargs and "call" in kwargs
        super(Pad, self).__init__(**kwargs)


class SrcPad(Pad):
    """
    A pad that provides data through a buffer when asked
    """
    @initializer
    def __init__(self, **kwargs):
        super(SrcPad, self).__init__(**kwargs)

    async def __call__(self):
        self.outbuf = self.call(pad = self)



class SinkPad(Pad):
    """
    A pad that receives data from a buffer when asked
    """
    @initializer
    def __init__(self, **kwargs):
        super(SinkPad, self).__init__(**kwargs)
    def link(self, other):
        """
	Only sink pads can be linked. A sink pad can be linked to only one
        source pad, but that source pad may be linked to other sink pads
        """
        self.other = other
        return {self: set((other,))}
    async def __call__(self):
        self.inbuf = self.other.outbuf
        self.call(self, self.inbuf)

class Element(Base):
    """
    A basic container to hold src and sink pads. The assmption is that this
    will be a base class for code that actually does something. It should never be
    subclassed directly, instead subclas SrcElement, SinkElement or
    TransformElement
    """

    @initializer
    def __init__(self, **kwargs):
        super(Element, self).__init__(**kwargs)
        self.graph = {}
    def link(self, other, **kwargs):
        sink_pads_by_name = {p.name:p for p in self.sink_pads}
        if isinstance(other, Pad):
            src_pad = other
        else:
            assert len(other.src_pads) == 1
            src_pad = other.src_pads[0]
        if "sink_pad_name" not in kwargs:
            assert len(self.sink_pads) == 1
            sink_pad = self.sink_pads[0]
        else:
            sink_pad = sink_pads_by_name[kwargs["sink_pad_name"]]
        self.graph.update(sink_pad.link(src_pad))

class SrcElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "src_pads" in kwargs
        super(SrcElement, self).__init__(**kwargs)
        self.graph.update({s: set() for s in self.src_pads})
        self.src_pad_dict = {p.name:p for p in self.src_pads}

class TransformElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "src_pads" in kwargs and "sink_pads" in kwargs
        super(TransformElement, self).__init__(**kwargs)
        self.graph.update({s: set(self.sink_pads) for s in self.src_pads})
        self.src_pad_dict = {p.name:p for p in self.src_pads}
        self.sink_pad_dict = {p.name:p for p in self.sink_pads}
        

class SinkElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "sink_pads" in kwargs
        super(SinkElement, self).__init__(**kwargs)
        self.graph = {}
        self.sink_pad_dict = {p.name:p for p in self.sink_pads}
