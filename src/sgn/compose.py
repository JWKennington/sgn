"""Composable elements for SGN.

This module provides functionality for composing multiple elements into a single
element that behaves as one atomic unit. This enables:
- Source + Transform(s) → ComposedSourceElement
- Transform + Transform(s) → ComposedTransformElement
- Transform(s) + Sink → ComposedSinkElement

The composed elements work by merging internal element graphs into the Pipeline's
graph, letting the Pipeline's single TopologicalSorter handle all execution.

Example usage:
    >>> from sgn.compose import Compose
    >>>
    >>> # Create a composed source from source + transforms
    >>> composed = (
    ...     Compose()
    ...     .connect(my_source, transform1)
    ...     .connect(transform1, transform2)
    ...     .as_source(name="my_composed_source")
    ... )
    >>>
    >>> # Use like any other source element
    >>> pipeline.connect(composed, my_sink)
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field

from .apps import Graph
from .base import (
    Element,
    InternalPad,
    Pad,
    SinkElement,
    SinkPad,
    SourceElement,
    SourcePad,
    TransformElement,
    UniqueID,
)
from .frames import Frame


class ComposedElementMixin(ABC):
    """Mixin class providing composition functionality for composed elements.

    This mixin provides the infrastructure for managing composed elements by:
    1. Building a merged graph from internal elements
    2. Identifying boundary pads (entry/exit points)
    3. Creating virtual source pads for frame injection (for Transform/Sink)

    The key insight is that composed elements don't execute internally - they
    merge their internal graphs into the Pipeline, which handles all execution.
    """

    # These will be set by the dataclass in the concrete class
    internal_elements: list[Element]
    internal_links: dict[str, str]

    # Runtime state (initialized in _build_internal_infrastructure)
    _internal_graph: Graph
    _boundary_source_pads: dict[str, SourcePad]
    _boundary_sink_pads: dict[str, SinkPad]
    _virtual_sources: dict[str, SourcePad]

    def _build_internal_infrastructure(self) -> None:
        """Build the internal graph and identify boundary pads.

        This method:
        1. Creates a Graph to manage internal elements
        2. Inserts all internal elements
        3. Links internal pads according to internal_links
        4. Identifies boundary pads (unlinked entry/exit points)
        """
        self._internal_graph = Graph()
        self._virtual_sources = {}

        # Insert all internal elements
        for element in self.internal_elements:
            self._internal_graph.insert(element)

        # Apply internal links
        for sink_pad_name, source_pad_name in self.internal_links.items():
            sink_pad = self._internal_graph[sink_pad_name]
            source_pad = self._internal_graph[source_pad_name]
            assert isinstance(sink_pad, SinkPad)
            assert isinstance(source_pad, SourcePad)
            self._internal_graph.link({sink_pad: source_pad})

        # Identify boundary pads
        self._identify_boundary_pads()

    def _identify_boundary_pads(self) -> None:
        """Find pads that should be exposed externally.

        Boundary source pads: source pads from internal elements that are NOT
            linked to any internal sink pad (these become outputs)
        Boundary sink pads: sink pads from internal elements that are NOT
            linked from any internal source pad (these become inputs)
        """
        internally_linked_sinks = set(self.internal_links.keys())
        internally_linked_sources = set(self.internal_links.values())

        self._boundary_source_pads = {}
        self._boundary_sink_pads = {}

        for element in self.internal_elements:
            # Check source pads - unlinked ones become outputs
            if isinstance(element, (SourceElement, TransformElement)):
                for pad_name, pad in element.srcs.items():
                    if pad.name not in internally_linked_sources:
                        self._boundary_source_pads[pad_name] = pad

            # Check sink pads - unlinked ones become inputs
            if isinstance(element, (TransformElement, SinkElement)):
                for pad_name, pad in element.snks.items():
                    if pad.name not in internally_linked_sinks:
                        self._boundary_sink_pads[pad_name] = pad


@dataclass(repr=False, kw_only=True)
class ComposedSourceElement(ComposedElementMixin, SourceElement):
    """A composed element that behaves like a SourceElement.

    Created from: One or more SourceElements, optionally with TransformElements.
    Exposes: Source pads from the boundary (unlinked source pads become outputs).

    The internal elements' graphs are merged into the Pipeline's graph.
    When executed, the Pipeline runs all internal elements via topological sort,
    and this element's source pads return the internal boundary pads' outputs.

    Supports both linear chains and non-linear graphs (e.g., multiple sources
    feeding into a merge transform).
    """

    # Composition inputs
    internal_elements: list[Element] = field(default_factory=list)
    internal_links: dict[str, str] = field(default_factory=dict)

    # Override to not require source_pad_names at init
    source_pad_names: list[str] = field(default_factory=list)

    # Runtime state
    _internal_graph: Graph = field(init=False)
    _boundary_source_pads: dict[str, SourcePad] = field(
        init=False, default_factory=dict
    )
    _boundary_sink_pads: dict[str, SinkPad] = field(init=False, default_factory=dict)
    _virtual_sources: dict[str, SourcePad] = field(init=False, default_factory=dict)

    def __post_init__(self):
        # First, set up UniqueID
        UniqueID.__post_init__(self)

        # Validate composition
        if not self.internal_elements:
            raise ValueError("ComposedSourceElement requires at least one element")

        # Must have at least one SourceElement
        has_source = any(
            isinstance(elem, SourceElement) for elem in self.internal_elements
        )
        if not has_source:
            raise TypeError("ComposedSourceElement requires at least one SourceElement")

        # Cannot contain any SinkElements
        for elem in self.internal_elements:
            if isinstance(elem, SinkElement):
                raise TypeError(
                    f"ComposedSourceElement cannot contain SinkElement, "
                    f"got {elem.name}"
                )

        # Build internal infrastructure
        self._build_internal_infrastructure()

        # Set up source pads from boundary pads
        self.source_pad_names = list(self._boundary_source_pads.keys())

        # Create our own pads that delegate to internal boundary pads
        self.graph = {}
        self.internal_pad = InternalPad(name="inl", element=self, call=self.internal)

        self.source_pads = [
            SourcePad(name=pad_name, element=self, call=self.new)
            for pad_name in self.source_pad_names
        ]
        self.srcs = {n: p for n, p in zip(self.source_pad_names, self.source_pads)}
        self.rsrcs = {p: n for n, p in zip(self.source_pad_names, self.source_pads)}

        if not self.source_pads:  # pragma: no cover
            raise ValueError("ComposedSourceElement must have at least one source pad")

        # Build composed element's graph:
        # 1. Include all internal element graphs
        self.graph.update(self._internal_graph.graph)

        # 2. Composed internal_pad depends on internal boundary source pads
        internal_boundary_srcs = set(self._boundary_source_pads.values())
        self.graph[self.internal_pad] = internal_boundary_srcs

        # 3. Composed source pads depend on composed internal_pad
        self.graph.update({s: {self.internal_pad} for s in self.source_pads})

    @property
    def pad_list(self) -> list[Pad]:
        """Return all pads including internal element pads."""
        pads: list[Pad] = []
        # Add our own pads
        pads.extend(self.source_pads)
        pads.append(self.internal_pad)
        # Add all internal element pads
        for element in self.internal_elements:
            pads.extend(element.pad_list)
        return pads

    def internal(self) -> None:
        """No-op - Pipeline handles all execution via merged graph."""
        pass

    def new(self, pad: SourcePad) -> Frame:
        """Return output from internal boundary pad (already executed by Pipeline)."""
        internal_pad = self._boundary_source_pads[pad.pad_name]
        output = internal_pad.output
        assert output is not None, f"Internal pad {internal_pad.name} has no output"
        return output


@dataclass(repr=False, kw_only=True)
class ComposedTransformElement(ComposedElementMixin, TransformElement):
    """A composed element that behaves like a TransformElement.

    Created from: TransformElement → TransformElement* (one or more transforms)
    Exposes: Sink pads from first element, Source pads from last element

    Uses virtual source pads to inject frames into internal boundary sink pads.
    The Pipeline executes all internal elements via topological sort.
    """

    # Composition inputs
    internal_elements: list[Element] = field(default_factory=list)
    internal_links: dict[str, str] = field(default_factory=dict)

    # Override to not require pad names at init
    source_pad_names: list[str] = field(default_factory=list)
    sink_pad_names: list[str] = field(default_factory=list)

    # Runtime state
    _internal_graph: Graph = field(init=False)
    _boundary_source_pads: dict[str, SourcePad] = field(
        init=False, default_factory=dict
    )
    _boundary_sink_pads: dict[str, SinkPad] = field(init=False, default_factory=dict)
    _virtual_sources: dict[str, SourcePad] = field(init=False, default_factory=dict)

    def __post_init__(self):
        # First, set up UniqueID
        UniqueID.__post_init__(self)

        # Validate composition
        if not self.internal_elements:
            raise ValueError("ComposedTransformElement requires at least one element")

        for elem in self.internal_elements:
            if isinstance(elem, (SourceElement, SinkElement)):
                raise TypeError(
                    f"Transform composition can only contain TransformElements, "
                    f"got {type(elem).__name__}"
                )

        # Build internal infrastructure
        self._build_internal_infrastructure()

        # Set up pads from boundary pads
        self.sink_pad_names = list(self._boundary_sink_pads.keys())
        self.source_pad_names = list(self._boundary_source_pads.keys())

        # Initialize graph and internal pad
        self.graph = {}
        self.internal_pad = InternalPad(name="inl", element=self, call=self.internal)

        # Create our own sink pads
        self.sink_pads = [
            SinkPad(name=pad_name, element=self, call=self.pull)
            for pad_name in self.sink_pad_names
        ]

        # Create virtual source pads for frame injection
        # These hold frames that internal boundary sinks will read from
        # We'll add the graph edges after initializing self.graph
        for snk_name in self.sink_pad_names:
            virtual_src = SourcePad(
                name=f"_vs_{snk_name}",
                element=self,
                call=lambda pad: pad.output,  # Output is pre-set by pull()
            )
            self._virtual_sources[snk_name] = virtual_src

        # Create our own source pads
        self.source_pads = [
            SourcePad(name=pad_name, element=self, call=self.new)
            for pad_name in self.source_pad_names
        ]

        self.srcs = {n: p for n, p in zip(self.source_pad_names, self.source_pads)}
        self.snks = {n: p for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self.rsrcs = {p: n for n, p in zip(self.source_pad_names, self.source_pads)}
        self.rsnks = {p: n for n, p in zip(self.sink_pad_names, self.sink_pads)}

        if not self.source_pads or not self.sink_pads:  # pragma: no cover
            raise ValueError(
                "ComposedTransformElement must have both sink and source pads"
            )

        # Build composed element's graph:
        # 1. Include all internal element graphs
        self.graph.update(self._internal_graph.graph)

        # 2. Link internal boundary sinks to virtual sources and add edges
        for snk_name, virtual_src in self._virtual_sources.items():
            internal_snk = self._boundary_sink_pads[snk_name]
            link_graph = internal_snk.link(virtual_src)
            self.graph.update(link_graph)

        # 3. Virtual sources depend on composed sink pads
        for snk_name, virtual_src in self._virtual_sources.items():
            composed_snk = self.snks[snk_name]
            self.graph[virtual_src] = {composed_snk}

        # 4. Composed internal_pad depends on internal boundary source pads
        internal_boundary_srcs = set(self._boundary_source_pads.values())
        self.graph[self.internal_pad] = internal_boundary_srcs

        # 5. Composed source pads depend on composed internal_pad
        self.graph.update({s: {self.internal_pad} for s in self.source_pads})

    @property
    def pad_list(self) -> list[Pad]:
        """Return all pads including internal element pads and virtual sources."""
        pads: list[Pad] = []
        # Add our own pads
        pads.extend(self.source_pads)
        pads.extend(self.sink_pads)
        pads.append(self.internal_pad)
        # Add virtual sources
        pads.extend(self._virtual_sources.values())
        # Add all internal element pads
        for element in self.internal_elements:
            pads.extend(element.pad_list)
        return pads

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Inject frame into virtual source for internal boundary sink."""
        virtual_src = self._virtual_sources[pad.pad_name]
        virtual_src.output = frame

    def internal(self) -> None:
        """No-op - Pipeline handles all execution via merged graph."""
        pass

    def new(self, pad: SourcePad) -> Frame:
        """Return output from internal boundary pad (already executed by Pipeline)."""
        internal_pad = self._boundary_source_pads[pad.pad_name]
        output = internal_pad.output
        assert output is not None, f"Internal pad {internal_pad.name} has no output"
        return output


@dataclass(kw_only=True)
class ComposedSinkElement(ComposedElementMixin, SinkElement):
    """A composed element that behaves like a SinkElement.

    Created from: One or more SinkElements, optionally with TransformElements.
    Exposes: Sink pads from the boundary (unlinked sink pads become inputs).

    Uses virtual source pads to inject frames into internal boundary sink pads.
    The Pipeline executes all internal elements via topological sort.

    Supports both linear chains and non-linear graphs (e.g., a transform that
    fans out to multiple sinks).
    """

    # Composition inputs
    internal_elements: list[Element] = field(default_factory=list)
    internal_links: dict[str, str] = field(default_factory=dict)

    # Override to not require sink_pad_names at init
    sink_pad_names: list[str] = field(default_factory=list)

    # Runtime state
    _internal_graph: Graph = field(init=False)
    _boundary_source_pads: dict[str, SourcePad] = field(
        init=False, default_factory=dict
    )
    _boundary_sink_pads: dict[str, SinkPad] = field(init=False, default_factory=dict)
    _virtual_sources: dict[str, SourcePad] = field(init=False, default_factory=dict)
    _internal_sinks: list[SinkElement] = field(init=False, default_factory=list)

    def __post_init__(self):
        # First, set up UniqueID
        UniqueID.__post_init__(self)

        # Validate composition
        if not self.internal_elements:
            raise ValueError("ComposedSinkElement requires at least one element")

        # Must have at least one SinkElement
        self._internal_sinks = [
            elem for elem in self.internal_elements if isinstance(elem, SinkElement)
        ]
        if not self._internal_sinks:
            raise TypeError("ComposedSinkElement requires at least one SinkElement")

        # Cannot contain any SourceElements
        for elem in self.internal_elements:
            if isinstance(elem, SourceElement):
                raise TypeError(
                    f"ComposedSinkElement cannot contain SourceElement, "
                    f"got {elem.name}"
                )

        # Build internal infrastructure
        self._build_internal_infrastructure()

        # Set up sink pads from boundary pads
        self.sink_pad_names = list(self._boundary_sink_pads.keys())

        # Initialize graph and internal pad
        self.graph = {}
        self.internal_pad = InternalPad(name="inl", element=self, call=self.internal)

        # Create our own sink pads
        self.sink_pads = [
            SinkPad(name=pad_name, element=self, call=self.pull)
            for pad_name in self.sink_pad_names
        ]

        # Create virtual source pads for frame injection
        for snk_name in self.sink_pad_names:
            virtual_src = SourcePad(
                name=f"_vs_{snk_name}",
                element=self,
                call=lambda pad: pad.output,
            )
            self._virtual_sources[snk_name] = virtual_src

        self.snks = {n: p for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self.rsnks = {p: n for n, p in zip(self.sink_pad_names, self.sink_pads)}
        self._at_eos = {p: False for p in self.sink_pads}

        if not self.sink_pads:  # pragma: no cover
            raise ValueError("ComposedSinkElement must have at least one sink pad")

        self.sink_pad_names_full = [p.name for p in self.sink_pads]

        # Build composed element's graph:
        # 1. Include all internal element graphs
        self.graph.update(self._internal_graph.graph)

        # 2. Link internal boundary sinks to virtual sources and add edges
        for snk_name, virtual_src in self._virtual_sources.items():
            internal_snk = self._boundary_sink_pads[snk_name]
            link_graph = internal_snk.link(virtual_src)
            self.graph.update(link_graph)

        # 3. Virtual sources depend on composed sink pads
        for snk_name, virtual_src in self._virtual_sources.items():
            composed_snk = self.snks[snk_name]
            self.graph[virtual_src] = {composed_snk}

        # 4. Composed internal_pad depends on all internal sinks' internal_pads
        # This ensures composed element waits for all internal sinks to complete
        internal_sink_pads = {sink.internal_pad for sink in self._internal_sinks}
        self.graph[self.internal_pad] = internal_sink_pads

    @property
    def pad_list(self) -> list[Pad]:
        """Return all pads including internal element pads and virtual sources."""
        pads: list[Pad] = []
        # Add our own pads
        pads.extend(self.sink_pads)
        pads.append(self.internal_pad)
        # Add virtual sources
        pads.extend(self._virtual_sources.values())
        # Add all internal element pads
        for element in self.internal_elements:
            pads.extend(element.pad_list)
        return pads

    @property
    def at_eos(self) -> bool:
        """Return True when all internal sinks are at EOS."""
        if self._internal_sinks:
            return all(sink.at_eos for sink in self._internal_sinks)
        return any(self._at_eos.values())  # pragma: no cover

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Inject frame into virtual source for internal boundary sink."""
        virtual_src = self._virtual_sources[pad.pad_name]
        virtual_src.output = frame

        # Propagate EOS if needed
        if frame.EOS:
            self.mark_eos(pad)

    def internal(self) -> None:
        """No-op - Pipeline handles all execution via merged graph."""
        pass


class Compose(Graph):
    """Fluent builder for creating composed elements.

    This class provides a chainable API for composing multiple elements
    into a single element. It inherits from Graph to reuse insert() and connect()
    methods with implicit linking strategies.

    Example (linear chain):
        >>> composed_source = (
        ...     Compose()
        ...     .connect(source, transform1)
        ...     .connect(transform1, transform2)
        ...     .as_source(name="my_source")
        ... )

    Example (non-linear graph):
        >>> composed_source = (
        ...     Compose()
        ...     .connect(source1, merge_transform)
        ...     .connect(source2, merge_transform)
        ...     .as_source(name="merged_source")
        ... )
    """

    def __init__(self, first_element: Element | None = None) -> None:
        """Initialize composition, optionally with a first element.

        Args:
            first_element: Optional first element for backwards compatibility
                with linear chain pattern. If None, use insert() to add elements.
        """
        super().__init__()
        if first_element is not None:
            self.insert(first_element)

    def _build_link_dict(self) -> dict[str, str]:
        """Extract link dictionary from internal graph.

        Returns a mapping of sink pad full names to source pad full names
        for all inter-element links.
        """
        links = {}
        for pad in self.graph:
            if isinstance(pad, SinkPad) and pad.other is not None:
                links[pad.name] = pad.other.name
        return links

    def as_source(self, name: str = "") -> ComposedSourceElement:
        """Finalize the composition as a ComposedSourceElement.

        Args:
            name: Optional name for the composed element

        Returns:
            A new ComposedSourceElement wrapping the composition
        """
        return ComposedSourceElement(
            name=name,
            internal_elements=self.elements.copy(),
            internal_links=self._build_link_dict(),
        )

    def as_transform(self, name: str = "") -> ComposedTransformElement:
        """Finalize the composition as a ComposedTransformElement.

        Args:
            name: Optional name for the composed element

        Returns:
            A new ComposedTransformElement wrapping the composition
        """
        return ComposedTransformElement(
            name=name,
            internal_elements=self.elements.copy(),
            internal_links=self._build_link_dict(),
        )

    def as_sink(self, name: str = "") -> ComposedSinkElement:
        """Finalize the composition as a ComposedSinkElement.

        Args:
            name: Optional name for the composed element

        Returns:
            A new ComposedSinkElement wrapping the composition
        """
        return ComposedSinkElement(
            name=name,
            internal_elements=self.elements.copy(),
            internal_links=self._build_link_dict(),
        )
