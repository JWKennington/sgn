from .. sources import *
from .. transforms import *
from .. sinks import *
from .. base import Base
import graphlib
import asyncio
import queue

class Pipeline(object):

    src_elements = sources_registry
    transform_elements = transforms_registry 
    sink_elements = sinks_registry 

    def __init__(self):
        """
        Class to establish and excecute a graph of elements that will process buffers.

        Registers methods to produce src, transform and sink elements and to assemble those elements
        in a directed acyclic graph.  Also establishes an event loop.
        """
        self.graph = {}
        self.loop = asyncio.get_event_loop()
        self.sinks = {}

        for method in self.src_elements + self.transform_elements + self.sink_elements:
            def _f(self = self, method = method, **kwargs):
                elem = eval("%s(**kwargs)" % method)
                self.link(elem.link_map)
                self.graph.update(elem.graph)
                if method in self.sink_elements:
                    self.sinks[elem.name] = elem
                return self
            setattr(self, method, _f)

    def link(self, link_map = {}):
        """
        link src pads to a sink pads with link_map = {"sink1":"src1", "sink2":"src2", "sink3":src1, ...} 
        """
        for sink_name, src_name in link_map.items():
            self.graph.update(Base.registry[sink_name].link(Base.registry[src_name]))
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
        """
        Run the pipeline until End Of Stream (EOS)
        """
        return self.loop.run_until_complete(self.__execute_graphs())
