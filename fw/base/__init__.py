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
    @initializer
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def __repr__(self):
        return str(self.kwargs)

class Base(object):
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
    @initializer
    def __init__(self, **kwargs):
        assert "element" in kwargs and "call" in kwargs
        super(Pad, self).__init__(**kwargs)


class SrcPad(Pad):
    @initializer
    def __init__(self, **kwargs):
        super(SrcPad, self).__init__(**kwargs)

    async def __call__(self):
        self.outbuf = self.call()



class SinkPad(Pad):
    @initializer
    def __init__(self, **kwargs):
        super(SinkPad, self).__init__(**kwargs)
    def link(self, other):
        self.other = other
        return {self: set((other,))}
    async def __call__(self):
        self.inbuf = self.other.outbuf
        self.call(self.inbuf)

class Element(Base):

    @initializer
    def __init__(self, **kwargs):
        super(Element, self).__init__(**kwargs)
        self.graph = {}

    def link(self, other):
        # someday figure out how to have multiple pads
        assert len(self.sink_pads) == 1 and len(other.src_pads) == 1
        self.graph.update(self.sink_pads[0].link(other.src_pads[0]))

class SrcElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "src_pads" in kwargs
        super(SrcElement, self).__init__(**kwargs)
        self.graph.update({s: set() for s in self.src_pads})

class TransformElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "src_pads" in kwargs and "sink_pads" in kwargs
        super(TransformElement, self).__init__(**kwargs)
        self.graph.update({s: set(self.sink_pads) for s in self.src_pads})
        

class SinkElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "sink_pads" in kwargs
        super(SinkElement, self).__init__(**kwargs)
        self.graph = {}
