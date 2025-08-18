"""Tests for the groups module functionality."""

import pytest

from sgn.sources import IterSource
from sgn.sinks import NullSink
from sgn.groups import group, select


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
    selected = sources.select("src1", "src3")

    assert len(selected.items) == 2
    assert src1 in selected.items
    assert src3 in selected.items
    assert src2 not in selected.items


def test_element_group_select_with_pad_selection():
    """Test ElementGroup select with PadSelections."""
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1", "V1"])

    selection = select(src2, "L1")
    sources = group(src1, selection)
    selected = sources.select("src2")

    assert len(selected.items) == 1
    assert selection in selected.items


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
