import graphlib
import asyncio
from initializer import initializer

class Buffer(object):
    @initializer
    def __init__(self, **kwargs):
        pass

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
        super(Pad, self).__init__(**kwargs)


class SrcPad(Pad):
    @initializer
    def __init__(self, **kwargs):
        super(SrcPad, self).__init__(**kwargs)

class SinkPad(Pad):
    @initializer
    def __init__(self, **kwargs):
        super(SinkPad, self).__init__(**kwargs)
    def link(self, other):
        self.other = other
        return {self: set((other,))}

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

class FakeHtSrcPad(SrcPad):
    def __call__(self):
        self.outbuf = Buffer()

class FakeHtSinkPad(SinkPad):
    pass

class FakeHtSrc(SrcElement):
    @initializer
    def __init__(self, **kwargs):
        kwargs["src_pads"] = [FakeHtSrcPad(name = "%s:src" % kwargs["name"])]
        super(FakeHtSrc, self).__init__(**kwargs)


class FakeTransformSrcPad(SrcPad):
    def __call__(self):
        self.outbuf = Buffer()

class FakeTransformSinkPad(SinkPad):
    def __call__(self):
        self.inbuf = self.other.outbuf

class FakeTransform(TransformElement):
    def __init__(self, **kwargs):
        kwargs["sink_pads"] = [FakeTransformSinkPad(name = "%s:sink" % kwargs["name"])]
        kwargs["src_pads"] = [FakeTransformSrcPad(name = "%s:src" % kwargs["name"])]
        super(FakeTransform, self).__init__(**kwargs)


class FakeOutputSinkPad(SinkPad):
    def __call__(self):
        self.outbuf = self.other.outbuf
        print (self.other.outbuf)

class FakeOutput(SinkElement):
    def __init__(self, **kwargs):
        kwargs["sink_pads"] = [FakeOutputSinkPad(name = "%s:sink" % kwargs["name"])]
        super(FakeOutput, self).__init__(**kwargs)

class Pipeline(object):

    src_elements = ("FakeHtSrc",)
    transform_elements = ("FakeTransform",)
    sink_elements = ("FakeOutput",)

    @initializer
    def __init__(self, **kwargs):
        self.head = None
        self.graph = {}
        self.loop = asyncio.get_event_loop()

        for method in self.src_elements:
            def _f(method = method, **kwargs):
                self.head = eval("%s(**kwargs)" % method)
                self.graph.update(self.head.graph)
                return self
            setattr(self, method, _f)
        for method in self.transform_elements:
            def _f(method = method, **kwargs):
                t = eval("%s(**kwargs)" % method)
                t.link(self.head)
                self.graph.update(t.graph)
                self.head = t
                return self
            setattr(self, method, _f)
        for method in self.sink_elements:
            def _f(method = method, **kwargs):
                s = eval("%s(**kwargs)" % method)
                s.link(self.head)
                self.graph.update(s.graph)
                return self
            setattr(self, method, _f)

    def __create_graph(self):
        return graphlib.TopologicalSorter(self.graph) 

    async def __execute_graph(self):
        # Replace with some other stopping condition
        while True:
            for node in self.__create_graph().static_order():
                node()

    def run(self):
        return self.loop.run_until_complete(self.__execute_graph())
        

pipeline = Pipeline()

pipeline.FakeHtSrc(
           name = "fake"
         ).FakeTransform(
           name = "transform"
         ).FakeOutput(
           name = "out"
         )

pipeline.run()

#graph = pipeline.create_graph()
#
#for node in graph.static_order():
#    print ("executing", node)
#    node()
