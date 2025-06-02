"""Pipeline class and related utilities to establish and execute a graph of element
tasks."""

from __future__ import annotations

import asyncio
import graphlib
import sys
from pathlib import Path
from typing import Dict, Optional, Union

from sgn import SourceElement, TransformElement
from sgn.base import (
    SGN_LOG_LEVELS,
    Element,
    ElementLike,
    InternalPad,
    Pad,
    SinkElement,
    SinkPad,
    SourcePad,
    get_sgn_logger,
)
from sgn.profile import async_sgn_mem_profile
from sgn.visualize import visualize

LOGGER = get_sgn_logger("pipeline", SGN_LOG_LEVELS)


class Pipeline:
    """A Pipeline is essentially a directed acyclic graph of tasks that process frames.

    These tasks are grouped using Pads and Elements. The Pipeline class is responsible
    for registering methods to produce source, transform and sink elements and to
    assemble those elements in a directed acyclic graph. It also establishes an event
    loop to execute the graph asynchronously.
    """

    def __init__(self) -> None:
        """Class to establish and execute a graph of elements that will process frames.

        Registers methods to produce source, transform and sink elements and to assemble
        those elements in a directed acyclic graph. Also establishes an event loop.
        """
        self._registry: dict[str, Union[Pad, Element]] = {}
        self.graph: dict[Pad, set[Pad]] = {}
        self.loop = asyncio.get_event_loop()
        self.__loop_counter = 0
        self.sinks: dict[str, SinkElement] = {}
        self.elements: list[Element] = []

    def __getitem__(self, name):
        """return a pipeline element or pad by name"""
        return self._registry[name]

    def insert(
        self,
        *elements: Element,
        link_map: Optional[dict[Union[str, SinkPad], Union[str, SourcePad]]] = None,
    ) -> Pipeline:
        """Insert element(s) into the pipeline.

        Args:
            *elements:
                Iterable[Element], the ordered elements to insert into the pipeline
            link_map:
                Optional[dict[Union[str, SinkPad], Union[str, SourcePad]]],
                a mapping of source pad to sink pad names to link

        Returns:
            Pipeline, the pipeline with the elements inserted
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
                if (
                    pad is not None
                ):  # Stupid mypy kludge, remove once python3.9 is dropped
                    assert (
                        pad.name not in self._registry
                    ), f"Pad name '{pad.name}' is already in use in this pipeline"
                    self._registry[pad.name] = pad
            if isinstance(element, SinkElement):
                self.sinks[element.name] = element
            self.graph.update(element.graph)
            self.elements.append(element)
        if link_map is not None:
            self.link(link_map)
        return self

    def link(
        self, link_map: Dict[Union[str, SinkPad], Union[str, SourcePad]]
    ) -> Pipeline:
        """Link pads in a pipeline.

        Args:
            link_map:
                dict[str, str], a mapping of sink pad to source pad names to link, note
                that the keys of the dictionary are the source pad names and the
                values are the sink pad names, so that: the data flows from value -> key
        """
        for sink_pad_name, source_pad_name in link_map.items():
            if isinstance(sink_pad_name, str):
                sink_pad = self._registry[sink_pad_name]
            else:
                sink_pad = sink_pad_name
            if isinstance(source_pad_name, str):
                source_pad = self._registry[source_pad_name]
            else:
                source_pad = source_pad_name

            assert isinstance(sink_pad, SinkPad), f"not a sink pad: {sink_pad}"
            assert isinstance(source_pad, SourcePad), f"not a source pad: {source_pad}"

            graph = sink_pad.link(source_pad)
            self.graph.update(graph)

        return self

    def nodes(self, pads: bool = True, intra: bool = False) -> tuple[str, ...]:
        """Get the nodes in the pipeline.

        Args:
            pads:
                bool, whether to include pads in the graph. If True, the graph will only
                consist of pads. If False, the graph will consist only of elements.
            intra:
                bool, default False, whether or not to include intra-element edges,
                e.g. from an element's sink pads to its source pads. In this case,
                whether to include Internal Pads in the graph.

        Returns:
            list[str], the nodes in the pipeline
        """
        # TODO remove this kludge when Python3.9 support is dropped
        element_types = [TransformElement, SinkElement, SourceElement, ElementLike]
        if sys.version_info < (3, 10):
            element_types = [SinkElement, SourceElement, TransformElement]

        if pads:
            pad_types = [SinkPad, SourcePad]
            if intra:
                pad_types.append(InternalPad)

            return tuple(
                sorted(
                    [
                        pad.name
                        for pad in self._registry.values()
                        if isinstance(pad, tuple(pad_types))
                    ]
                )
            )
        return tuple(
            sorted(
                [
                    element.name
                    for element in self._registry.values()
                    if isinstance(element, tuple(element_types))
                ]
            )
        )

    def edges(
        self, pads: bool = True, intra: bool = False
    ) -> tuple[tuple[str, str], ...]:
        """Get the edges in the pipeline.

        Args:
            pads:
                bool, whether to include pads in the graph. If True, the graph will only
                consist of pads. If False, the graph will consist only of elements.
            intra:
                bool, default False, whether or not to include intra-element edges, e.g.
                from an element's sink pads to its source pads

        Returns:
        """
        edges = set()
        for target, sources in self.graph.items():
            for source in sources:
                if not intra and isinstance(source, (SinkPad, InternalPad)):
                    continue

                if pads:
                    edges.add((source.name, target.name))
                else:
                    source_element = source.element
                    target_element = target.element
                    edges.add((source_element.name, target_element.name))
        return tuple(sorted(edges))

    def to_graph(self, label: str | None = None):
        """graphviz.DiGraph representation of pipeline

        Args:
            label:
                str, label for the graph

        Returns:
            DiGraph, the graph object
        """
        return visualize(self, label=label)

    def to_dot(self, label: str | None = None) -> str:
        """Convert the pipeline to a graph using graphviz.

        Args:
            label:
                str, label for the graph

        Returns:
            str, the graph representation of the pipeline
        """
        return visualize(self, label=label).source

    def visualize(self, path: str, label: str | None = None) -> None:
        """Convert the pipeline to a graph using graphviz, then render into a visual
        file.

        Args:
            path:
                str, the relative or full path to the file to write the graph to
            label:
                str, label for the graph
        """
        visualize(self, label=label, path=Path(path))

    @async_sgn_mem_profile(LOGGER)
    async def __execute_graph_loop(self) -> None:
        async def _partial(node):
            try:
                return await node()
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                msg = f"(from pad '{node.name}'): {exc_value}."
                raise exc_type(msg) from e

        self.__loop_counter += 1
        LOGGER.info("Executing graph loop %s:", self.__loop_counter)
        ts = graphlib.TopologicalSorter(self.graph)
        ts.prepare()
        while ts.is_active():
            # concurrently execute the next batch of ready nodes
            nodes = ts.get_ready()
            tasks = [
                self.loop.create_task(_partial(node)) for node in nodes  # type: ignore # noqa: E501
            ]
            await asyncio.gather(*tasks)
            ts.done(*nodes)

    async def _execute_graphs(self) -> None:
        """Async graph execution function."""

        while not all(sink.at_eos for sink in self.sinks.values()):
            await self.__execute_graph_loop()

    def check(self) -> None:
        """Check that pipeline elements are connected.

        Throws an RuntimeError exception if unconnected pads are
        encountered.

        """
        if not self.sinks:
            msg = "Pipeline contains no sink elements."
            raise RuntimeError(msg)
        for element in self.elements:
            for source_pad in element.source_pads:
                if not source_pad.is_linked:
                    msg = f"Source pad not linked: {source_pad}"
                    raise RuntimeError(msg)
            for sink_pad in element.sink_pads:
                if not sink_pad.is_linked:
                    msg = f"Sink pad not linked: {sink_pad}"
                    raise RuntimeError(msg)

    def run(self) -> None:
        """Run the pipeline until End Of Stream (EOS)"""
        self.check()
        if not self.loop.is_running():
            self.loop.run_until_complete(self._execute_graphs())
        else:
            """If the event loop is running, e.g., running in a Jupyter
            Notebook, run the pipeline in a forked thread.
            """
            import threading

            def _run_in_fork(pipeline):
                pipeline.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(pipeline.loop)
                pipeline.loop.run_until_complete(pipeline._execute_graphs())
                pipeline.loop.close()

            thread = threading.Thread(target=_run_in_fork, args=(self,))
            thread.start()
            thread.join()
