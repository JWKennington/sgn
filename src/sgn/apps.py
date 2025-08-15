"""Pipeline class and related utilities to establish and execute a graph of element
tasks."""

from __future__ import annotations

import asyncio
import graphlib
import logging
import sys
from pathlib import Path
from typing import Dict, Optional, Union

from sgn import SourceElement, TransformElement
from sgn.base import (
    Element,
    ElementLike,
    InternalPad,
    Pad,
    SinkElement,
    SinkPad,
    SourcePad,
)
from sgn.groups import ElementGroup, PadSelection
from sgn.logger import configure_sgn_logging
from sgn.profile import async_sgn_mem_profile
from sgn.visualize import visualize

logger = logging.getLogger("sgn.pipeline")


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

    def connect(
        self,
        source: Union[Element, ElementGroup, PadSelection],
        sink: Union[Element, ElementGroup, PadSelection],
        link_map: Optional[Dict[str, str]] = None,
    ) -> Pipeline:
        """Connect elements, ElementGroups, or PadSelections using implicit linking.

        This method supports multiple linking patterns:
        1. Element-to-element linking with implicit pad matching:
           pipeline.connect(source_element, sink_element)
        2. Element-to-element linking with explicit mapping:
           pipeline.connect(source_element, sink_element, link_map={"sink": "source"})
        3. ElementGroup linking (supports elements and pad selections):
           pipeline.connect(group(s1, s2), sink_element)
           pipeline.connect(group(source, select(element, "pad1")), sink)
        4. Direct PadSelection linking:
           pipeline.connect(select(source, "pad1"), sink_element)

        Implicit linking strategies (when no link_map provided):
        1. Exact match: Connect when source and sink pad names are identical
        2. Partial match: Connect all matching pad names (ignores non-matching pads)
        3. N-to-1: Single sink pad, connect all source pads to it
        4. 1-to-N: Single source pad, connect to all sink pads

        Args:
            source:
                Element, ElementGroup, or PadSelection, the source for linking
            sink:
                Element, ElementGroup, or PadSelection, the sink for linking
            link_map:
                dict[str, str], optional, explicit mapping of sink pad names to
                source pad names.

        Returns:
            Pipeline: The pipeline with the new links added.

        Raises:
            ValueError: If implicit linking strategy is ambiguous.
            TypeError: If arguments are of unexpected types.
        """
        if isinstance(source, SinkElement):
            msg = f"Source '{source.name}' is a SinkElement and has no source pads"
            raise ValueError(msg)
        if isinstance(sink, SourceElement):
            msg = f"Sink '{sink.name}' is a SourceElement and has no sink pads"
            raise ValueError(msg)

        source_pads = source.srcs
        sink_pads = sink.snks

        # Ensure all elements are inserted in pipeline
        def ensure_elements_inserted(obj):
            if isinstance(obj, (SourceElement, TransformElement, SinkElement)):
                if obj.name not in self._registry:
                    self.insert(obj)
            elif isinstance(obj, ElementGroup):
                for element in obj.elements:
                    if element.name not in self._registry:
                        self.insert(element)
            elif isinstance(obj, PadSelection):
                if obj.element.name not in self._registry:
                    self.insert(obj.element)

        ensure_elements_inserted(source)
        ensure_elements_inserted(sink)

        return self._connect_pads(source_pads, sink_pads, link_map)

    def _connect_pads(
        self,
        source_pads: Dict[str, SourcePad],
        sink_pads: Dict[str, SinkPad],
        link_map: Optional[Dict[str, str]] = None,
    ) -> Pipeline:
        """Connect source and sink pads using implicit linking strategies."""
        resolved_link_map: Dict[Union[str, SinkPad], Union[str, SourcePad]]
        source_pad_names = set(source_pads.keys())
        sink_pad_names = set(sink_pads.keys())

        # Determine linking strategy
        if link_map:
            # Explicit mapping provided
            resolved_link_map = {}
            for sink_pad_name, source_pad_name in link_map.items():
                if sink_pad_name not in sink_pads:
                    msg = f"sink pad '{sink_pad_name}' not found"
                    raise KeyError(msg)
                if source_pad_name not in source_pads:
                    msg = f"source pad '{source_pad_name}' not found"
                    raise KeyError(msg)

                sink_pad = sink_pads[sink_pad_name]
                source_pad = source_pads[source_pad_name]
                resolved_link_map[sink_pad] = source_pad

            return self.link(resolved_link_map)

        elif source_pad_names == sink_pad_names:
            # One-to-one linking strategy: same pad names
            resolved_link_map = {
                sink_pads[name]: source_pads[name] for name in source_pad_names
            }
            return self.link(resolved_link_map)

        elif source_pad_names & sink_pad_names:
            # Partial matching strategy: connect all matching pad names
            matching_names = source_pad_names & sink_pad_names
            resolved_link_map = {
                sink_pads[name]: source_pads[name] for name in matching_names
            }
            return self.link(resolved_link_map)

        elif len(sink_pad_names) == 1:
            # N-to-one linking strategy
            sink_pad = next(iter(sink_pads.values()))
            for source_pad in source_pads.values():
                self.link({sink_pad: source_pad})
            return self

        elif len(source_pad_names) == 1:
            # One-to-N linking strategy
            source_pad = next(iter(source_pads.values()))
            resolved_link_map = {
                sink_pad: source_pad for sink_pad in sink_pads.values()
            }
            return self.link(resolved_link_map)

        else:
            msg = (
                "unable to determine unambiguous linking strategy from source "
                "and sink pads. an explicit link_map is required."
            )
            raise ValueError(msg)

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
        element_types = [TransformElement, SinkElement, SourceElement]
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

    @async_sgn_mem_profile(logger)
    async def __execute_graph_loop(self) -> None:
        async def _partial(node):
            try:
                return await node()
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                msg = f"(from pad '{node.name}'): {exc_value}."
                raise exc_type(msg) from e

        self.__loop_counter += 1
        logger.info("Executing graph loop %s:", self.__loop_counter)
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

    def run(self, auto_parallelize: bool = True) -> None:
        """Run the pipeline until End Of Stream (EOS)

        Args:
            auto_parallelize: If True (default), automatically detects if
            parallelization is needed and handles it transparently. If False,
            runs the pipeline normally without parallelization detection.
        """
        configure_sgn_logging()
        if auto_parallelize:
            # Import here to avoid circular imports
            from sgn.subprocess import Parallelize

            # Use automatic parallelization detection
            if Parallelize.needs_parallelization(self):
                with Parallelize(self) as parallelize:
                    parallelize.run()
                return

        # Run normally without parallelization
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
