"""Tests for the groups module functionality."""

import pytest

from sgn.sources import IterSource
from sgn.sinks import NullSink
from sgn.transforms import CallableTransform
from sgn.groups import group, select, PadSelection


def test_pad_selection_creation():
    """Test creating a PadSelection."""
    src = IterSource(name="src", source_pad_names=["H1", "L1", "V1"])
    selection = select(src, "H1", "L1")

    assert selection.element is src
    assert selection.pad_names == {"H1", "L1"}


def test_pad_selection_validation():
    """Test that PadSelection validates pad names exist."""
    src = IterSource(name="src", source_pad_names=["H1", "L1"])

    # Valid selection should work
    selection = select(src, "H1")
    assert "H1" in selection.pad_names

    # Invalid selection should fail
    with pytest.raises(ValueError, match="Pad names {'V1'} not found"):
        select(src, "V1")


def test_pad_selection_srcs():
    """Test PadSelection srcs property."""
    src = IterSource(name="src", source_pad_names=["H1", "L1", "V1"])
    selection = select(src, "H1", "V1")

    srcs = selection.srcs
    assert len(srcs) == 2
    assert "H1" in srcs
    assert "V1" in srcs
    assert "L1" not in srcs  # Not selected

    # Verify these are actual SourcePad objects
    assert srcs["H1"] is src.srcs["H1"]
    assert srcs["V1"] is src.srcs["V1"]

    # Test that SinkElement returns empty srcs
    sink = NullSink(name="sink", sink_pad_names=["data"])
    sink_selection = select(sink, "data")
    assert sink_selection.srcs == {}  # SinkElement has no source pads


def test_pad_selection_snks():
    """Test PadSelection snks property."""
    sink = NullSink(name="sink", sink_pad_names=["in1", "in2", "in3"])
    selection = select(sink, "in1", "in3")

    snks = selection.snks
    assert len(snks) == 2
    assert "in1" in snks
    assert "in3" in snks
    assert "in2" not in snks  # Not selected

    # Test that SourceElement returns empty snks
    src = IterSource(name="src", source_pad_names=["H1"])
    src_selection = select(src, "H1")
    assert src_selection.snks == {}  # SourceElement has no sink pads


def test_pad_selection_elements():
    """Test PadSelection elements property."""
    src = IterSource(name="src", source_pad_names=["H1"])
    selection = select(src, "H1")

    assert selection.elements == [src]


def test_element_group_creation():
    """Test creating an ElementGroup."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1"])

    # Create group
    sources = group(src1, src2)
    assert len(sources.items) == 2
    assert src1 in sources.items
    assert src2 in sources.items


def test_element_group_with_pad_selection():
    """Test ElementGroup with PadSelection."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1", "V1"])

    # Create group with element and pad selection
    selection = select(src2, "L1")
    sources = group(src1, selection)

    assert len(sources.items) == 2
    assert src1 in sources.items
    assert selection in sources.items


def test_element_group_nested():
    """Test ElementGroup with nested ElementGroups."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1"])
    src3 = IterSource(name="src3", source_pad_names=["V1"])

    # Create nested groups
    group1 = group(src1, src2)
    group2 = group(group1, src3)

    # Should flatten to individual items
    assert len(group2.items) == 3
    assert src1 in group2.items
    assert src2 in group2.items
    assert src3 in group2.items


def test_element_group_select():
    """Test ElementGroup select method."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1"])
    src3 = IterSource(name="src3", source_pad_names=["V1"])

    sources = group(src1, src2, src3)
    selected = sources.select("H1", "V1")

    assert len(selected.items) == 2
    # Should create PadSelections for src1 (H1) and src3 (V1)
    assert all(isinstance(item, PadSelection) for item in selected.items)

    # Find the selections
    h1_selection = next(item for item in selected.items if "H1" in item.pad_names)
    v1_selection = next(item for item in selected.items if "V1" in item.pad_names)

    assert h1_selection.element is src1
    assert h1_selection.pad_names == {"H1"}
    assert v1_selection.element is src3
    assert v1_selection.pad_names == {"V1"}


def test_element_group_select_with_pad_selection():
    """Test ElementGroup select with PadSelections."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1", "V1"])

    selection = select(src2, "L1")
    sources = group(src1, selection)
    selected = sources.select("L1")

    assert len(selected.items) == 1
    # Should create a PadSelection from the existing PadSelection with L1 pad
    selected_item = selected.items[0]
    assert isinstance(selected_item, PadSelection)
    assert selected_item.element is src2
    assert selected_item.pad_names == {"L1"}


def test_element_group_elements():
    """Test ElementGroup elements property."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1"])

    # Test with elements
    sources = group(src1, src2)
    elements = sources.elements
    assert len(elements) == 2
    assert src1 in elements
    assert src2 in elements

    # Test with pad selection
    selection = select(src2, "L1")
    mixed = group(src1, selection)
    elements = mixed.elements
    assert len(elements) == 2  # Should include src2 from selection
    assert src1 in elements
    assert src2 in elements


def test_element_group_srcs():
    """Test ElementGroup srcs property."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1", "V1"])

    # Test with full elements
    sources = group(src1, src2)
    srcs = sources.srcs
    assert len(srcs) == 3
    assert "H1" in srcs
    assert "L1" in srcs
    assert "V1" in srcs

    # Test with pad selection
    selection = select(src2, "L1")
    mixed = group(src1, selection)
    srcs = mixed.srcs
    assert len(srcs) == 2
    assert "H1" in srcs
    assert "L1" in srcs
    assert "V1" not in srcs  # Not selected


def test_element_group_srcs_duplicate_error():
    """Test that duplicate pad names in group raise error."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["H1"])  # Same pad name

    sources = group(src1, src2)
    with pytest.raises(KeyError, match="Duplicate pad name 'H1'"):
        _ = sources.srcs


def test_element_group_snks():
    """Test ElementGroup snks property."""
    sink1 = NullSink(name="sink1", sink_pad_names=["H1"])
    sink2 = NullSink(name="sink2", sink_pad_names=["L1", "V1"])

    sinks = group(sink1, sink2)
    snks = sinks.snks
    assert len(snks) == 3
    assert "H1" in snks
    assert "L1" in snks
    assert "V1" in snks

    # Test duplicate sink pad names raise error
    sink3 = NullSink(name="sink3", sink_pad_names=["H1"])  # Same pad name as sink1
    duplicate_sinks = group(sink1, sink3)
    with pytest.raises(KeyError, match="Duplicate pad name 'H1'"):
        _ = duplicate_sinks.snks


def test_element_group_wrong_type_error():
    """Test ElementGroup with wrong element type for pad extraction."""
    src = IterSource(name="src", source_pad_names=["H1"])
    sink = NullSink(name="sink", sink_pad_names=["L1"])

    # Test srcs with sink element should fail
    mixed_for_srcs = group(src, sink)
    with pytest.raises(
        ValueError, match="Element 'sink' is a SinkElement and has no source pads"
    ):
        _ = mixed_for_srcs.srcs

    # Test snks with source element should fail
    mixed_for_snks = group(src, sink)
    with pytest.raises(
        ValueError, match="Element 'src' is a SourceElement and has no sink pads"
    ):
        _ = mixed_for_snks.snks


def test_group_function_invalid_type():
    """Test group() function with invalid types."""
    with pytest.raises(
        TypeError, match="Expected Element, PadSelection, or ElementGroup"
    ):
        group("invalid_type")


def test_select_on_pad_selection():
    """Test using select() on a PadSelection to narrow it down."""
    src = IterSource(name="src", source_pad_names=["H1", "L1", "V1"])

    # Create initial selection with multiple pads
    initial_selection = select(src, "H1", "L1", "V1")
    assert initial_selection.pad_names == {"H1", "L1", "V1"}

    # Narrow it down to just H1
    narrow_selection = select(initial_selection, "H1")
    assert narrow_selection.element is src
    assert narrow_selection.pad_names == {"H1"}

    # Narrow it down to H1 and L1
    narrow_selection2 = select(initial_selection, "H1", "L1")
    assert narrow_selection2.element is src
    assert narrow_selection2.pad_names == {"H1", "L1"}


def test_select_on_pad_selection_no_match():
    """Test using select() on a PadSelection with no matching pads raises error."""
    src = IterSource(name="src", source_pad_names=["H1", "L1"])

    # Create selection with H1 only
    initial_selection = select(src, "H1")

    # Try to select V1 which is not available
    with pytest.raises(ValueError, match="No matching pads found"):
        select(initial_selection, "V1")


def test_select_on_element_group():
    """Test using select() on an ElementGroup."""
    src = IterSource(name="src", source_pad_names=["H1", "L1"])
    sink = NullSink(name="sink", sink_pad_names=["L1", "V1"])

    # Create group with both source and sink
    mixed = group(src, sink)

    # Select L1 pads from the group
    selected = select(mixed, "L1")

    assert len(selected.items) == 2
    assert all(isinstance(item, PadSelection) for item in selected.items)

    # Find the selections for each element
    l1_selections = {item.element.name: item for item in selected.items}

    assert "src" in l1_selections
    assert "sink" in l1_selections
    assert l1_selections["src"].pad_names == {"L1"}
    assert l1_selections["sink"].pad_names == {"L1"}


def test_select_invalid_type():
    """Test select() with invalid input type."""
    with pytest.raises(
        TypeError, match="Expected Element, PadSelection, or ElementGroup"
    ):
        select("invalid_type", "pad1")


def test_pad_selection_iteration_methods():
    """Test PadSelection select_by_source and select_by_sink iteration."""
    # Test with a TransformElement that has both source and sink pads
    transform = CallableTransform.from_callable(
        name="transform",
        callable=lambda *args: args[0],  # identity function
        output_pad_name="out",
        sink_pad_names=["in1", "in2"],
    )
    selection = select(transform, "out", "in1", "in2")

    # Test select_by_source
    source_items = list(selection.select_by_source())
    assert len(source_items) == 1
    pad_names = {pad_name for pad_name, _ in source_items}
    assert pad_names == {"out"}

    for pad_name, single_selection in source_items:
        assert isinstance(single_selection, PadSelection)
        assert single_selection.element is transform
        assert single_selection.pad_names == {pad_name}

    # Test select_by_sink on same element
    sink_items = list(selection.select_by_sink())
    assert len(sink_items) == 2
    sink_pad_names = {pad_name for pad_name, _ in sink_items}
    assert sink_pad_names == {"in1", "in2"}

    for pad_name, single_selection in sink_items:
        assert isinstance(single_selection, PadSelection)
        assert single_selection.element is transform
        assert single_selection.pad_names == {pad_name}


def test_element_group_iteration_methods():
    """Test ElementGroup select_by_source and select_by_sink iteration."""
    # Test select_by_source with source elements only
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1", "V1"])
    source_group = group(src1, src2)

    source_items = list(source_group.select_by_source())
    assert len(source_items) == 3  # H1, L1, V1
    pad_names = {pad_name for pad_name, _ in source_items}
    assert pad_names == {"H1", "L1", "V1"}

    for pad_name, single_selection in source_items:
        assert isinstance(single_selection, PadSelection)
        assert single_selection.pad_names == {pad_name}
        if pad_name == "H1":
            assert single_selection.element is src1
        else:
            assert single_selection.element is src2

    # Test select_by_sink with sink elements only
    sink1 = NullSink(name="sink1", sink_pad_names=["in1"])
    sink2 = NullSink(name="sink2", sink_pad_names=["in2", "in3"])
    sink_group = group(sink1, sink2)

    sink_items = list(sink_group.select_by_sink())
    assert len(sink_items) == 3  # in1, in2, in3
    sink_pad_names = {pad_name for pad_name, _ in sink_items}
    assert sink_pad_names == {"in1", "in2", "in3"}

    for pad_name, single_selection in sink_items:
        assert isinstance(single_selection, PadSelection)
        assert single_selection.pad_names == {pad_name}
        if pad_name == "in1":
            assert single_selection.element is sink1
        else:
            assert single_selection.element is sink2


def test_iteration_methods_no_pads_error():
    """Test iteration methods raise ValueError when no pads available."""
    src = IterSource(name="src", source_pad_names=["H1"])
    sink = NullSink(name="sink", sink_pad_names=["in1"])

    src_selection = select(src, "H1")
    sink_selection = select(sink, "in1")

    # Source elements have no sink pads
    with pytest.raises(ValueError, match="No sink pads available"):
        list(src_selection.select_by_sink())

    # Sink elements have no source pads
    with pytest.raises(ValueError, match="No source pads available"):
        list(sink_selection.select_by_source())
