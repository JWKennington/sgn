from __future__ import annotations

import asyncio
import graphlib
from typing import Optional, Union

from .base import Element, ElementLike, Pad, SinkElement, SinkPad, SourcePad


class Pipeline:
    def __init__(self) -> None:
        """
        Class to establish and excecute a graph of elements that will process
        frames.

        Registers methods to produce source, transform and sink elements and to
        assemble those elements in a directed acyclic graph.  Also establishes
        an event loop.
        """
        self._registry: dict[str, Union[Pad, Element]] = {}
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
                element, ElementLike
            ), f"Element {element} is not an instance of a sgn.Element"
            assert (
                element.name not in self._registry
            ), f"Element name '{element.name}' is already in use in this pipeline"
            self._registry[element.name] = element
            for pad in element.pad_list:
                assert (
                    pad.name not in self._registry
                ), f"Pad name '{pad.name}' is already in use in this pipeline"
                self._registry[pad.name] = pad
            if isinstance(element, SinkElement):
                self.sinks[element.name] = element
            self.graph.update(element.graph)
        if link_map is not None:
            self.link(link_map)
        return self

    def link(self, link_map: dict[str, str]) -> Pipeline:
        """
        link source pads to a sink pads with
        link_map = {sink_pad_name:src_pad_name, ...}
        """
        for sink_pad_name, source_pad_name in link_map.items():
            sink_pad = self._registry[sink_pad_name]
            source_pad = self._registry[source_pad_name]
            assert isinstance(sink_pad, SinkPad)
            assert isinstance(source_pad, SourcePad)

            graph = sink_pad.link(source_pad)
            self.graph.update(graph)

        return self

    async def _execute_graphs(self) -> None:
        while not all(sink.at_eos for sink in self.sinks.values()):
            ts = graphlib.TopologicalSorter(self.graph)
            ts.prepare()
            while ts.is_active():
                # concurrently execute the next batch of ready nodes
                nodes = ts.get_ready()
                tasks = [self.loop.create_task(node()) for node in nodes]  # type: ignore # noqa: E501
                await asyncio.gather(*tasks)
                ts.done(*nodes)

    def run(self) -> None:
        """
        Run the pipeline until End Of Stream (EOS)
        """
        self.loop.run_until_complete(self._execute_graphs())
