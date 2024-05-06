from .. sources import *
from .. transforms import *
from .. sinks import *
import graphlib
import asyncio
import queue

class Pipeline(object):

    src_elements = sources_registry
    transform_elements = transforms_registry 
    sink_elements = sinks_registry 

    @initializer
    def __init__(self, **kwargs):
        self.head = None
        self.graph = {}
        self.loop = asyncio.get_event_loop()
        self.sources = {} 
        self.sinks = {}
        self.transforms = {}
        self.src_pads = {}
        self.sink_pads = {} # FIXME populate this

        for method in self.src_elements:
            def _f(method = method, **kwargs):
                self.head = eval("%s(**kwargs)" % method)
                self.sources[self.head.name] = self.head
                self.src_pads.update(self.head.src_pad_dict)
                self.graph.update(self.head.graph)
                return self
            setattr(self, method, _f)
        for method in self.transform_elements:
            def _f(method = method, **kwargs):
                t = eval("%s(**kwargs)" % method)
                h = self.head if "src_pad_name" not in kwargs else self.src_pads[kwargs["src_pad_name"]]
                t.link(h, **kwargs)
                self.graph.update(t.graph)
                self.head = t
                self.transforms[self.head.name] = self.head
                self.src_pads.update(self.head.src_pad_dict)
                self.sink_pads.update(self.head.sink_pad_dict)
                return self
            setattr(self, method, _f)
        for method in self.sink_elements:
            def _f(method = method, **kwargs):
                s = eval("%s(**kwargs)" % method)
                h = self.head if "src_pad_name" not in kwargs else self.src_pads[kwargs["src_pad_name"]]
                s.link(h, **kwargs)
                self.sinks[s.name] = s
                self.sink_pads.update(s.sink_pad_dict)
                self.head = None
                self.graph.update(s.graph)
                return self
            setattr(self, method, _f)

    def link(self, **kwargs):
        assert "src_pad_name" in kwargs and "sink_pad_name" in kwargs
        self.graph.update(self.sink_pads[kwargs["sink_pad_name"]].link(self.src_pads[kwargs["src_pad_name"]]))
        return self 

    async def __execute_graphs(self):
        # FIXME can we remove the outer while true and somehow use asyncio to schedule these in succession?
        while not all(e.EOS for e in self.sinks.values()):
            ts = graphlib.TopologicalSorter(self.graph)
            ts.prepare()
            done_nodes = queue.Queue() # blocks by default
            while ts.is_active():
                for node in ts.get_ready():
                    task = self.loop.create_task(node())
                    def callback(task, ts = ts, node = node, done_nodes = done_nodes):
                        ts.done(node)
                        done_nodes.put(node)
                    task.add_done_callback(callback)
                    await task
                done_nodes.get() # blocks until at least one thing is done

    def run(self):
        return self.loop.run_until_complete(self.__execute_graphs())
