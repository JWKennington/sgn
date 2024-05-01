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

    async def __execute_graphs(self):
        # FIXME can we remove the outer while true and somehow use asyncio to schedule these in succession?
        while True:
             ts = graphlib.TopologicalSorter(self.graph)
             ts.prepare()
	     # FIXME still not quite right, this will only parallelize over
	     # ready nodes and doesn't allow the possibility that one of those
	     # nodes might allow others to run...
             while ts.is_active():
                 tasks = []
                 for node in ts.get_ready():
                     task = self.loop.create_task(node())
                     def callback(task, ts = ts, node = node):
                         ts.done(node)
                     task.add_done_callback(callback)
                     tasks.append(task)
                 await asyncio.gather(*tasks)

    def run(self):
        return self.loop.run_until_complete(self.__execute_graphs())
