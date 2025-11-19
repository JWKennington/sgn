"""Unit tests for the base module."""

import asyncio
import random
from dataclasses import dataclass

import pytest

from sgn.base import (
    ElementLike,
    Frame,
    SinkElement,
    SinkPad,
    SourceElement,
    SourcePad,
    TransformElement,
    UniqueID,
)
from sgn.frames import DataSpec


def asyncio_run(coro):
    """Run an asyncio coroutine."""
    return asyncio.get_event_loop().run_until_complete(coro)


@dataclass(frozen=True)
class RateDataSpec(DataSpec):
    rate: int


class TestUniqueID:
    """Test group for the UniqueID class."""

    def test_init(self):
        """Test the UniqueID class constructor."""
        ui = UniqueID()
        assert ui._id
        assert ui.name == ui._id

        ui = UniqueID(name="test")
        assert ui.name == "test"

    def test_hash(self):
        """Test the __hash__ method."""
        ui = UniqueID()
        assert hash(ui) == hash(ui._id)

    def test_eq(self):
        """Test the __eq__ method."""
        ui1 = UniqueID()
        ui2 = UniqueID()
        assert ui1 == ui1
        assert ui1 != ui2


class TestSinkPadDataSpecValidation:
    """Test group for data specification validation in SinkPad."""

    def test_data_spec_consistency_check(self):
        """Test that SinkPad validates data spec consistency across frames."""

        def dummy_src(pad):
            # Return frame with random rate each time
            spec = RateDataSpec(rate=random.randint(1, 2048))
            return Frame(spec=spec)

        def dummy_snk(pad, frame):
            return None

        # Create a mock element for the pads
        mock_element = ElementLike(name="mock")

        p1 = SourcePad(
            name="testsrc", element=mock_element, call=dummy_src, output=None
        )
        p2 = SinkPad(name="testsink", element=mock_element, call=dummy_snk, input=None)

        # Link
        p2.link(p1)

        # Run correct order first time - should succeed
        asyncio_run(p1())
        asyncio_run(p2())
        assert p2.input is not None
        assert p2.data_spec is not None

        # Run again, data specification will be different
        asyncio_run(p1())
        with pytest.raises(
            ValueError, match="inconsistent with previously received frames"
        ):
            asyncio_run(p2())

    def test_data_spec_initial_set(self):
        """Test that SinkPad sets data_spec on first call."""

        def dummy_src(pad):
            return Frame(spec=RateDataSpec(rate=100))

        def dummy_snk(pad, frame):
            return None

        mock_element = ElementLike(name="mock")

        p1 = SourcePad(
            name="testsrc", element=mock_element, call=dummy_src, output=None
        )
        p2 = SinkPad(name="testsink", element=mock_element, call=dummy_snk, input=None)

        # Initially data_spec should be None
        assert p2.data_spec is None

        # Link and run
        p2.link(p1)
        asyncio_run(p1())
        asyncio_run(p2())

        # Now data_spec should be set
        assert p2.data_spec == RateDataSpec(rate=100)

    def test_sink_pad_call_not_linked(self):
        """Test that calling unlinked SinkPad raises AssertionError."""
        mock_element = ElementLike(name="mock")
        p2 = SinkPad(
            name="testsink", element=mock_element, call=lambda p, f: None, input=None
        )

        # Try running before linking (bad)
        with pytest.raises(AssertionError, match="Sink pad has not been linked"):
            asyncio_run(p2())

    def test_sink_pad_call_wrong_order(self):
        """Test that calling SinkPad before SourcePad raises AssertionError."""

        def dummy_src(pad):
            return Frame()

        def dummy_snk(pad, frame):
            return None

        mock_element = ElementLike(name="mock")

        p1 = SourcePad(
            name="testsrc", element=mock_element, call=dummy_src, output=None
        )
        p2 = SinkPad(name="testsink", element=mock_element, call=dummy_snk, input=None)

        # Link
        p2.link(p1)

        # Run wrong order (sink before source has output)
        with pytest.raises(AssertionError):
            asyncio_run(p2())

    def test_sink_pad_link_wrong_type(self):
        """Test that linking a sink pad to non-SourcePad raises AssertionError."""
        mock_element = ElementLike(name="mock")
        s2 = SinkPad(name="testsink", element=mock_element, call=None, input=None)

        # Catch error for linking wrong item
        with pytest.raises(AssertionError, match="not an instance of SourcePad"):
            s2.link(None)

    def test_sink_pad_link_success(self):
        """Test successful linking of sink pad to source pad."""
        mock_element = ElementLike(name="mock")
        s1 = SourcePad(name="testsrc", element=mock_element, call=None, output=None)
        s2 = SinkPad(name="testsink", element=mock_element, call=None, input=None)

        assert s2.other is None
        res = s2.link(s1)
        assert s2.other == s1
        assert res == {s2: {s1}}


class TestElementLikeProperties:
    """Test group for ElementLike properties."""

    def test_source_pad_dict(self):
        """Test the source_pad_dict property."""
        mock_element = ElementLike(name="mock")
        src = SourcePad(name="testsrc", element=mock_element, call=None, output=None)
        el = ElementLike(name="element", source_pads=[src])

        pad_dict = el.source_pad_dict
        # The pad name will be formatted as "element:src:testsrc"
        assert len(pad_dict) == 1
        assert src in pad_dict.values()
        assert src.name in pad_dict

    def test_sink_pad_dict(self):
        """Test the sink_pad_dict property."""
        mock_element = ElementLike(name="mock")
        snk = SinkPad(name="testsink", element=mock_element, call=None, input=None)
        el = ElementLike(name="element", sink_pads=[snk])

        pad_dict = el.sink_pad_dict
        # The pad name will be formatted as "element:snk:testsink"
        assert len(pad_dict) == 1
        assert snk in pad_dict.values()
        assert snk.name in pad_dict

    def test_pad_list(self):
        """Test the pad_list property includes all pads."""
        mock_element = ElementLike(name="mock")
        src = SourcePad(name="testsrc", element=mock_element, call=None, output=None)
        snk = SinkPad(name="testsink", element=mock_element, call=None, input=None)
        el = ElementLike(name="element", source_pads=[src], sink_pads=[snk])

        # Pad list will have source pads, sink pads, and internal pad
        pad_list = el.pad_list
        assert len(pad_list) == 3
        assert src in pad_list
        assert snk in pad_list
        # Internal pad should be included
        assert el.internal_pad in pad_list

    def test_logger_property(self):
        """Test that logger property returns scoped logger."""
        el = ElementLike(name="test_element")
        logger = el.logger

        # Logger should be scoped to the element name
        assert logger.name == "sgn.test_element"


class TestSourceElement:
    """Test group for SourceElement class."""

    def test_source_element_initialization(self):
        """Test SourceElement initializes with source pads."""

        class DummySource(SourceElement):
            def new(self, pad):
                return Frame()

        src = DummySource(name="test_src", source_pad_names=["out1", "out2"])

        # Should have 2 source pads
        assert len(src.source_pads) == 2
        assert len(src.sink_pads) == 0

        # Check short names dictionaries
        assert "out1" in src.srcs
        assert "out2" in src.srcs
        assert src.source_pads[0] in src.rsrcs
        assert src.source_pads[1] in src.rsrcs

        # Check graph structure
        assert len(src.graph) == 2
        for s in src.source_pads:
            assert s in src.graph
            assert src.internal_pad in src.graph[s]

    def test_source_element_no_pads_assertion(self):
        """Test SourceElement raises assertion if no source pads specified."""

        class DummySource(SourceElement):
            def new(self, pad):
                return Frame()

        with pytest.raises(
            AssertionError, match="SourceElement must specify source pads"
        ):
            DummySource(name="test_src", source_pad_names=[])


class TestTransformElement:
    """Test group for TransformElement class."""

    def test_transform_element_initialization(self):
        """Test TransformElement initializes with both source and sink pads."""

        class DummyTransform(TransformElement):
            def pull(self, pad, frame):
                pass

            def new(self, pad):
                return Frame()

        xform = DummyTransform(
            name="test_xform",
            source_pad_names=["out1", "out2"],
            sink_pad_names=["in1", "in2"],
        )

        # Should have both source and sink pads
        assert len(xform.source_pads) == 2
        assert len(xform.sink_pads) == 2

        # Check short names dictionaries
        assert "out1" in xform.srcs
        assert "out2" in xform.srcs
        assert "in1" in xform.snks
        assert "in2" in xform.snks
        assert xform.source_pads[0] in xform.rsrcs
        assert xform.sink_pads[0] in xform.rsnks

        # Check graph structure (bipartite: sinks -> internal -> sources)
        assert xform.internal_pad in xform.graph
        assert set(xform.sink_pads) == xform.graph[xform.internal_pad]
        for s in xform.source_pads:
            assert s in xform.graph
            assert xform.internal_pad in xform.graph[s]

    def test_transform_element_no_source_pads_assertion(self):
        """Test TransformElement raises assertion if no source pads."""

        class DummyTransform(TransformElement):
            def pull(self, pad, frame):
                pass

            def new(self, pad):
                return Frame()

        with pytest.raises(
            AssertionError,
            match="TransformElement must specify both sink and source pads",
        ):
            DummyTransform(
                name="test_xform", source_pad_names=[], sink_pad_names=["in1"]
            )

    def test_transform_element_no_sink_pads_assertion(self):
        """Test TransformElement raises assertion if no sink pads."""

        class DummyTransform(TransformElement):
            def pull(self, pad, frame):
                pass

            def new(self, pad):
                return Frame()

        with pytest.raises(
            AssertionError,
            match="TransformElement must specify both sink and source pads",
        ):
            DummyTransform(
                name="test_xform", source_pad_names=["out1"], sink_pad_names=[]
            )


class TestSinkElement:
    """Test group for SinkElement class."""

    def test_sink_element_initialization(self):
        """Test SinkElement initializes with sink pads."""

        class DummySink(SinkElement):
            def pull(self, pad, frame):
                pass

        snk = DummySink(name="test_snk", sink_pad_names=["in1", "in2"])

        # Should have 2 sink pads and no source pads
        assert len(snk.sink_pads) == 2
        assert len(snk.source_pads) == 0

        # Check short names dictionaries
        assert "in1" in snk.snks
        assert "in2" in snk.snks
        assert snk.sink_pads[0] in snk.rsnks
        assert snk.sink_pads[1] in snk.rsnks

        # Check EOS tracking
        assert all(not eos for eos in snk._at_eos.values())
        assert not snk.at_eos

        # Check sink_pad_names_full
        assert len(snk.sink_pad_names_full) == 2

        # Check graph structure (all sinks -> internal)
        assert snk.internal_pad in snk.graph
        assert set(snk.sink_pads) == snk.graph[snk.internal_pad]

    def test_sink_element_no_pads_assertion(self):
        """Test SinkElement raises assertion if no sink pads specified."""

        class DummySink(SinkElement):
            def pull(self, pad, frame):
                pass

        with pytest.raises(AssertionError, match="SinkElement must specify sink pads"):
            DummySink(name="test_snk", sink_pad_names=[])

    def test_sink_element_mark_eos(self):
        """Test SinkElement mark_eos method."""

        class DummySink(SinkElement):
            def pull(self, pad, frame):
                pass

        snk = DummySink(name="test_snk", sink_pad_names=["in1", "in2"])

        # Initially not at EOS
        assert not snk.at_eos

        # Mark one pad as EOS
        snk.mark_eos(snk.sink_pads[0])
        assert snk._at_eos[snk.sink_pads[0]]
        assert snk.at_eos  # Should propagate

        # Other pad still not at EOS
        assert not snk._at_eos[snk.sink_pads[1]]

    def test_sink_element_at_eos_property(self):
        """Test SinkElement at_eos property returns True if any pad is at EOS."""

        class DummySink(SinkElement):
            def pull(self, pad, frame):
                pass

        snk = DummySink(name="test_snk", sink_pad_names=["in1", "in2"])

        # Initially not at EOS
        assert not snk.at_eos

        # Mark one pad
        snk._at_eos[snk.sink_pads[0]] = True
        assert snk.at_eos

        # Mark both pads
        snk._at_eos[snk.sink_pads[1]] = True
        assert snk.at_eos
