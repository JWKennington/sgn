from __future__ import annotations

import asyncio
import graphlib
from queue import Queue
from typing import Optional

from ..base import Base, Element, Pad, SinkElement, SinkPad, SourcePad


class Pipeline:
    def __init__(self) -> None:
        """
        Class to establish and excecute a graph of elements that will process
        buffers.

        Registers methods to produce source, transform and sink elements and to
        assemble those elements in a directed acyclic graph.  Also establishes
        an event loop.
        """
        self.graph: dict[SourcePad, set[SinkPad]] = {}
        self.loop = asyncio.get_event_loop()
        self.sinks: dict[str, SinkElement] = {}

    def insert(
        self, *elements: Element, link_map: Optional[dict[str, str]] = None
    ) -> Pipeline:
        """
        Insert element(s) into the pipeline
        """
        for element in elements:
            assert isinstance(
                element, Element
            ), f"Element {element} is not an instance of a sgn.Element"
            self.graph.update(element.graph)
            if isinstance(element, SinkElement):
                self.sinks[element.name] = element
        if link_map is not None:
            self.link(link_map)
        return self

    def link(self, link_map: dict[str, str]) -> Pipeline:
        """
        link source pads to a sink pads with link_map = {"sink1":"src1",
        "sink2":"src2", "sink3":src1, ...}
        """
        for sink_name, source_name in link_map.items():
            self.graph.update(Base.registry[sink_name].link(Base.registry[source_name]))
        return self

    async def __execute_graphs(self) -> None:
        # FIXME can we remove the outer while true and somehow use asyncio to
        # schedule these in succession?
        while not all(e.EOS for e in self.sinks.values()):
            ts = graphlib.TopologicalSorter(self.graph)
            ts.prepare()
            done_nodes: Queue[Pad] = Queue()  # blocks by default
            while ts.is_active():
                for node in ts.get_ready():
                    task = self.loop.create_task(node())

                    def callback(task, ts=ts, node=node, done_nodes=done_nodes):
                        ts.done(node)
                        done_nodes.put(node)

                    task.add_done_callback(callback)
                    await task
                done_nodes.get()  # blocks until at least one thing is done

    def run(self) -> None:
        """
        Run the pipeline until End Of Stream (EOS)
        """
        self.loop.run_until_complete(self.__execute_graphs())
