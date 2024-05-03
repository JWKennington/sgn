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
        """
        name must be in kwargs and must not have been used by any derived class instance
        """
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
    Pads are 1:1 with graph nodes but src and sink pads must be grouped into
    elements in order to exchange data from sink->src.  src->sink exchanges happen
    between elements.
    """
    @initializer
    def __init__(self, **kwargs):
        """
	A pad must belong to an element and that element must be provided as a
        keyword argument called "element".  The element must also provide a call
        function that will be executed when the pad is called
        """
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
        """
	When called, a source pad receives a buffer from the element that the
        pad belongs to.
        """
        self.outbuf = self.call(pad = self)



class SinkPad(Pad):
    """
    A pad that receives data from a buffer when asked.  When linked, it returns
    a dictionary suitable for building a graph in graphlib.
    """
    @initializer
    def __init__(self, **kwargs):
        super(SinkPad, self).__init__(**kwargs)
    def link(self, other):
        """
	Only sink pads can be linked. A sink pad can be linked to only one
	source pad, but multiple sink pads may link to the same src pad.
        Returns a dictionary of dependencies suitable for adding to a graphlib graph.
        """
        self.other = other
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

class Element(Base):
    """
    A basic container to hold src and sink pads. The assmption is that this
    will be a base class for code that actually does something. It should never be
    subclassed directly, instead subclass SrcElement, SinkElement or
    TransformElement
    """

    @initializer
    def __init__(self, **kwargs):
        super(Element, self).__init__(**kwargs)
        self.graph = {}
    def link(self, other, **kwargs):
        """
        A element links to another element only on its sink pads.  This function offers a few rules:

        1) If "other" is a source pad, it will be used
        2) otherwise, if "other" is an element it **must** have only one source pad 
        3) you must specify a sink pad name if this element instance has more than one sink pad.

	it updates the elements internal dictionary which is suitable for being
        added to a graphlib graph.
        """
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
        """
        "src_pads" (iterable) must be in kwargs.  Every source pad is added to the graph with no dependencies.
        """
        assert "src_pads" in kwargs
        super(SrcElement, self).__init__(**kwargs)
        self.graph.update({s: set() for s in self.src_pads})
        self.src_pad_dict = {p.name:p for p in self.src_pads}

class TransformElement(Element):
    @initializer
    def __init__(self, **kwargs):
        """
	Both "src_pads" and "sink_pads" must be in kwargs.  All sink pads
        depend on all source pads in a transform element. If you don't want that to be
        true, write more than one transform element.
        """
        assert "src_pads" in kwargs and "sink_pads" in kwargs
        super(TransformElement, self).__init__(**kwargs)
        self.graph.update({s: set(self.sink_pads) for s in self.src_pads})
        self.src_pad_dict = {p.name:p for p in self.src_pads}
        self.sink_pad_dict = {p.name:p for p in self.sink_pads}
        

class SinkElement(Element):
    @initializer
    def __init__(self, **kwargs):
        """
        "sink_pads" must be in kwargs
        """
        assert "sink_pads" in kwargs
        super(SinkElement, self).__init__(**kwargs)
        self.graph = {}
        self.sink_pad_dict = {p.name:p for p in self.sink_pads}
