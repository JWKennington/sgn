import graphlib
#import yaml
from enum import Flag, auto
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

class Element(Base):

    @initializer
    def __init__(self, **kwargs):
        super(Element, self).__init__(**kwargs)

    def link(self, other):
        assert len(self.sink_pads) == 1 and len(other.src_pads) == 1
        self.graph.update({self.sink_pads[0]: set((other.src_pads[0],))})

class SrcElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "src_pads" in kwargs
        super(SrcElement, self).__init__(**kwargs)
        self.graph = {s: set() for s in self.src_pads}

class TransformElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "src_pads" in kwargs and "sink_pads" in kwargs
        super(TransformElement, self).__init__(**kwargs)
        self.graph = {s: set(self.sink_pads) for s in self.src_pads}
        

class SinkElement(Element):
    @initializer
    def __init__(self, **kwargs):
        assert "sink_pads" in kwargs
        super(SinkElement, self).__init__(**kwargs)
        self.graph = {}

class FakeSrcPad(SrcPad):
    pass

class FakeSrc(SrcElement):
    @initializer
    def __init__(self, **kwargs):
        kwargs["src_pads"] = [FakeSrcPad(name = "%s:src" % kwargs["name"])]
        super(FakeSrc, self).__init__(**kwargs)

class Pipeline(object):

    src_elements = ("FakeSrc",)
    transform_elements = ("TransformElement",)
    sink_elements = ("SinkElement",)

    @initializer
    def __init__(self, **kwargs):
        self.head = None
        self.graph = {}

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

    def create_graph(self):
        return graphlib.TopologicalSorter(self.graph) 


pipeline = Pipeline()

pipeline.FakeSrc(
           name = "fake"
         ).TransformElement(
           name = "transform", src_pads = [SrcPad(name="transform_src_pad")], sink_pads = [SinkPad(name="transform_sink_pad")]
         ).SinkElement(
           name = "out", sink_pads = [SinkPad(name="out_sink_pad")]
         )

graph = pipeline.create_graph()

for node in graph.static_order():
    print (node)
