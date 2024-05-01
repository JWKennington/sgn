from .. sources import *
from .. transforms import *
from .. sinks import *
import graphlib
import asyncio


class Pipeline(object):

    src_elements = sources_registry
    transform_elements = transforms_registry 
    sink_elements = sinks_registry 

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
