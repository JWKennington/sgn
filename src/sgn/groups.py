"""Element grouping and pad selection operations.

This module provides functionality for grouping elements and selecting specific
pads for use in pipeline operations. It contains the core abstractions that allow
for flexible and intuitive pipeline construction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, Protocol, overload

from .base import (
    Element,
    SinkElement,
    SinkPad,
    SourceElement,
    SourcePad,
    TransformElement,
)


class PadProvider(Protocol):
    """Protocol defining the interface required by PadIteratorMixin."""

    @property
    def srcs(self) -> dict[str, SourcePad]:
        """Get source pads as a dictionary mapping pad names to SourcePad objects."""
        ...

    @property
    def snks(self) -> dict[str, SinkPad]:
        """Get sink pads as a dictionary mapping pad names to SinkPad objects."""
        ...

    @property
    def elements(self) -> list[Element]:
        """Get all elements referenced by this object."""
        ...


class PadIteratorMixin:
    """Mixin class that provides iteration methods for pad selections."""

    def select_by_source(self: PadProvider) -> Iterator[tuple[str, PadSelection]]:
        """Iterate over source pads, yielding (pad_name, single_pad_selection) tuples.

        Each yielded PadSelection contains only a single source pad.
        Raises ValueError if there are no source pads.
        """
        srcs = self.srcs
        if not srcs:
            msg = "No source pads available in this selection"
            raise ValueError(msg)

        for pad_name, pad in srcs.items():
            yield pad_name, PadSelection(element=pad.element, pad_names={pad_name})

    def select_by_sink(self: PadProvider) -> Iterator[tuple[str, PadSelection]]:
        """Iterate over sink pads, yielding (pad_name, single_pad_selection) tuples.

        Each yielded PadSelection contains only a single sink pad.
        Raises ValueError if there are no sink pads.
        """
        snks = self.snks
        if not snks:
            msg = "No sink pads available in this selection"
            raise ValueError(msg)

        for pad_name, pad in snks.items():
            yield pad_name, PadSelection(element=pad.element, pad_names={pad_name})


@dataclass
class PadSelection(PadIteratorMixin):
    """Represents a selection of specific pads from an element.

    This allows users to specify exactly which pads from an element
    should be used in grouping operations.
    """

    element: Element
    pad_names: set[str]

    def __post_init__(self) -> None:
        """Validate that the selected pad names exist on the element."""
        all_pad_names = set()

        # Add source pad names if element has them
        if isinstance(self.element, (SourceElement, TransformElement)):
            all_pad_names.update(self.element.srcs.keys())

        # Add sink pad names if element has them
        if isinstance(self.element, (TransformElement, SinkElement)):
            all_pad_names.update(self.element.snks.keys())

        invalid_names = self.pad_names - all_pad_names
        if invalid_names:
            msg = (
                f"Pad names {invalid_names} not found on element '{self.element.name}'"
                f" Pad names available: {all_pad_names}"
            )
            raise ValueError(msg)

    @property
    def srcs(self) -> dict[str, SourcePad]:
        """Extract selected source pads from the element using names as keys."""
        if isinstance(self.element, (SourceElement, TransformElement)):
            return {
                name: pad
                for name, pad in self.element.srcs.items()
                if name in self.pad_names
            }
        return {}

    @property
    def snks(self) -> dict[str, SinkPad]:
        """Extract selected sink pads from the element using names as keys."""
        if isinstance(self.element, (TransformElement, SinkElement)):
            return {
                name: pad
                for name, pad in self.element.snks.items()
                if name in self.pad_names
            }
        return {}

    @property
    def elements(self) -> list[Element]:
        """Get the element referenced by this selection."""
        return [self.element]


@dataclass
class ElementGroup(PadIteratorMixin):
    """A unified group for elements and pad selections.

    This class holds a collection of elements and pad selections without
    determining upfront whether to extract source or sink pads. The actual
    pad extraction is deferred until the group is used in pipeline.connect(),
    where the context (source vs sink position) determines which pads to extract.
    """

    items: list[Element | PadSelection] = field(default_factory=list)

    def select(self, *pad_names: str) -> ElementGroup:
        """Select pads from items in the group by pad name."""
        selected_items: list[Element | PadSelection] = []
        pad_names_set = set(pad_names)

        for item in self.items:
            if isinstance(item, PadSelection):
                # For existing pad selections, intersect with requested pad names
                element = item.element
                available_pads = item.pad_names
            else:
                # For elements, get all their pad names
                element = item
                available_pads = set()
                if isinstance(element, (SourceElement, TransformElement)):
                    available_pads.update(element.srcs.keys())
                if isinstance(element, (TransformElement, SinkElement)):
                    available_pads.update(element.snks.keys())

            # Find intersection of available pads with requested pad names
            matching_pads = available_pads & pad_names_set
            if matching_pads:
                selected_items.append(
                    PadSelection(element=element, pad_names=matching_pads)
                )

        return ElementGroup(items=selected_items)

    @property
    def elements(self) -> list[Element]:
        """Get all unique elements referenced by this group."""
        seen = set()
        unique_elements = []
        for item in self.items:
            if isinstance(item, PadSelection):
                element = item.element
            else:
                element = item
            element_id = id(element)
            if element_id not in seen:
                seen.add(element_id)
                unique_elements.append(element)
        return unique_elements

    @property
    def srcs(self) -> dict[str, SourcePad]:
        """Extract source pads from all items in the group using names as keys."""
        combined_pads = {}

        for item in self.items:
            if isinstance(item, SinkElement):
                msg = f"Element '{item.name}' is a SinkElement and has no source pads"
                raise ValueError(msg)

            for pad_name, pad in item.srcs.items():
                if pad_name in combined_pads:
                    msg = f"Duplicate pad name '{pad_name}' in group"
                    raise KeyError(msg)
                combined_pads[pad_name] = pad

        return combined_pads

    @property
    def snks(self) -> dict[str, SinkPad]:
        """Extract sink pads from all items in the group using names as keys."""
        combined_pads = {}

        for item in self.items:
            if isinstance(item, SourceElement):
                msg = f"Element '{item.name}' is a SourceElement and has no sink pads"
                raise ValueError(msg)

            for pad_name, pad in item.snks.items():
                if pad_name in combined_pads:
                    msg = f"Duplicate pad name '{pad_name}' in group"
                    raise KeyError(msg)
                combined_pads[pad_name] = pad

        return combined_pads


@overload
def select(target: Element, *pad_names: str) -> PadSelection: ...


@overload
def select(target: PadSelection, *pad_names: str) -> PadSelection: ...


@overload
def select(target: ElementGroup, *pad_names: str) -> ElementGroup: ...


def select(
    target: Element | PadSelection | ElementGroup, *pad_names: str
) -> PadSelection | ElementGroup:
    """Create or refine pad selections from elements, existing selections, or groups.

    Args:
        target: The element, pad selection, or element group to select pads from.
        *pad_names: Names of the pads to select.

    Returns:
        A PadSelection if target is Element or PadSelection,
        or ElementGroup if target is ElementGroup.

    Examples:
        >>> src = IterSource(name="src", source_pad_names=["H1", "L1", "V1"])
        >>> h1_only = select(src, "H1")
        >>> h1_l1_only = select(src, "H1", "L1")
        >>>
        >>> # Narrow an existing selection
        >>> h1_from_selection = select(h1_l1_only, "H1")
        >>>
        >>> # Select from a group
        >>> sources = group(src1, src2)
        >>> h1_from_group = select(sources, "H1")
    """
    match target:
        case SourceElement() | TransformElement() | SinkElement():
            return PadSelection(element=target, pad_names=set(pad_names))
        case PadSelection():
            # Narrow the existing selection by intersecting pad names
            new_pad_names = target.pad_names & set(pad_names)
            if not new_pad_names:
                msg = (
                    f"No matching pads found. Requested: {set(pad_names)}, "
                    f"Available: {target.pad_names}"
                )
                raise ValueError(msg)
            return PadSelection(element=target.element, pad_names=new_pad_names)
        case ElementGroup():
            return target.select(*pad_names)
        case _:
            msg = f"Expected Element, PadSelection, or ElementGroup, got {type(target)}"
            raise TypeError(msg)


def group(*items: Element | PadSelection | ElementGroup) -> ElementGroup:
    """Create a unified group from elements, pad selections, and existing groups.

    This function always returns an ElementGroup that holds elements and/or
    pad selections. The decision of which pads to extract is deferred until
    the group is used in pipeline.connect().

    Args:
        *items: Elements, pad selections, and/or existing ElementGroups to combine.

    Returns:
        A group containing all the provided items.

    Examples:
        >>> src1 = IterSource(name="src1", source_pad_names=["H1"])
        >>> src2 = IterSource(name="src2", source_pad_names=["L1", "V1"])
        >>> snk = Sink(name="snk", sink_pad_names=["H1", "L1"])
        >>>
        >>> # Group elements
        >>> sources = group(src1, src2)
        >>>
        >>> # Group with pad selection
        >>> l1_only = select(src2, "L1")
        >>> sources_with_selection = group(src1, l1_only)
        >>>
        >>> # Connect groups
        >>> pipeline.connect(sources_with_selection, snk)
    """
    all_items: list[Element | PadSelection] = []

    for item in items:
        if isinstance(item, PadSelection):
            all_items.append(item)
        elif isinstance(item, (SourceElement, TransformElement, SinkElement)):
            all_items.append(item)
        elif isinstance(item, ElementGroup):
            all_items.extend(item.items)
        else:
            msg = f"Expected Element, PadSelection, or ElementGroup, got {type(item)}"
            raise TypeError(msg)

    return ElementGroup(items=all_items)
