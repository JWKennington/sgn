"""Unit tests for the apps module."""

import pathlib
import tempfile
import asyncio
from collections import deque
from unittest import mock

import pytest

from sgn import NullSink, NullSource
from sgn.apps import Pipeline
from sgn.sinks import DequeSink
from sgn.sources import DequeSource
from sgn.transforms import CallableTransform

# Cross-compatibility with graphviz
try:
    import graphviz
except ImportError:
    graphviz = None


class TestPipeline:
    """Test group for Pipeline class."""

    def test_init(self):
        """Test Pipeline.__init__"""
        p = Pipeline()
        assert isinstance(p, Pipeline)
        assert p.graph == {}
        assert p._registry == {}
        assert p.sinks == {}

    def test_element_validation(self):
        """Test element validation."""
        p = Pipeline()

        with pytest.raises(RuntimeError):
            p.check()

        e1 = DequeSource(name="src1", source_pad_names=("H1",))
        e2 = DequeSource(name="src2", source_pad_names=("H1",))
        # Bad don't do this only checking for state
        e2.source_pads[0].name = e1.source_pads[0].name

        # Must be a valid element
        with pytest.raises(AssertionError):
            p.insert(None)

        p.insert(e1)

        # Must not be already in the pipeline
        with pytest.raises(AssertionError):
            p.insert(e1)

        with pytest.raises(AssertionError):
            p.insert(e2)

        e3 = NullSink(sink_pad_names=("H1",))
        p.insert(e3)
        with pytest.raises(RuntimeError):
            p.check()

        e4 = NullSink(sink_pad_names=("H1",))
        e5 = NullSink(sink_pad_names=("H1",))
        p.insert(
            e4,
            e5,
            link_map={
                e3.snks["H1"]: e2.srcs["H1"],
                e4.snks["H1"]: e1.srcs["H1"],
            },
        )
        with pytest.raises(RuntimeError):
            p.check()

    def test_run(self):
        """Test execute graphs."""
        p = Pipeline()
        snk = DequeSink(
            name="snk1",
            sink_pad_names=("H1",),
        )
        src = DequeSource(
            name="src1",
            source_pad_names=("H1",),
            # TODO add key formatting helper
            iters={"src1:src:H1": deque([1, 2, 3])},
        )
        p.insert(
            src,
            CallableTransform.from_callable(
                name="t1",
                sink_pad_names=("H1",),
                callable=lambda frame: None if frame.data is None else frame.data + 10,
                output_pad_name="H1",
            ),
            snk,
            link_map={
                "t1:snk:H1": src.srcs["H1"],
                snk.sink_pads[0]: "t1:src:H1",
            },
        )

        p.run()
        assert snk.deques["H1"] == deque([13, 12, 11])

    def test_run_with_exception_in_pipeline(self):
        """Test that exceptions in pipeline elements are properly handled."""
        p = Pipeline()
        src = DequeSource(
            name="src1",
            source_pad_names=("H1",),
            iters={"src1:src:H1": deque([1])},
        )
        snk = DequeSink(
            name="snk1",
            sink_pad_names=("H1",),
        )

        # Create a transform that will raise an exception
        def failing_transform(frame):
            if frame.data is not None:
                raise ValueError("Intentional test exception")
            return frame.data

        p.insert(
            src,
            CallableTransform.from_callable(
                name="failing_transform",
                sink_pad_names=("H1",),
                callable=failing_transform,
                output_pad_name="H1",
            ),
            snk,
            link_map={
                "failing_transform:snk:H1": src.srcs["H1"],
                snk.sink_pads[0]: "failing_transform:src:H1",
            },
        )

        # Run with auto_parallelize=False to force normal pipeline execution
        with pytest.raises(ValueError, match="Intentional test exception"):
            p.run(auto_parallelize=False)

    def test_run_while_a_running_event_loop_exist(self):
        """Test execute graphs while a running event loop exist."""
        loop = asyncio.get_event_loop()

        async def async_test_run():
            self.test_run()

        loop.run_until_complete(async_test_run())


class TestPipelineGraphviz:
    """Test group for Pipeline class with graphviz."""

    @pytest.fixture(autouse=True, scope="class")
    def pipeline(self) -> Pipeline:
        """Create sample pipeline for tests."""
        p = Pipeline()
        p.insert(
            NullSource(
                name="src1",
                source_pad_names=("H1",),
            ),
            CallableTransform.from_callable(
                name="t1",
                sink_pad_names=["H1"],
                callable=lambda frame: None,
                output_pad_name="H1",
            ),
            NullSink(
                name="snk1",
                sink_pad_names=("H1",),
            ),
            link_map={
                "t1:snk:H1": "src1:src:H1",
                "snk1:snk:H1": "t1:src:H1",
            },
        )
        return p

    def test_nodes(self, pipeline):
        """Test nodes."""
        assert pipeline.nodes() == (
            "snk1:snk:H1",
            "src1:src:H1",
            "t1:snk:H1",
            "t1:src:H1",
        )
        assert pipeline.nodes(intra=True) == (
            "snk1:inl:inl",
            "snk1:snk:H1",
            "src1:inl:inl",
            "src1:src:H1",
            "t1:inl:inl",
            "t1:snk:H1",
            "t1:src:H1",
        )
        with mock.patch("sys.version_info", (3, 9)):
            assert pipeline.nodes() == (
                "snk1:snk:H1",
                "src1:src:H1",
                "t1:snk:H1",
                "t1:src:H1",
            )

    def test_edges(self, pipeline):
        """Test edges."""
        assert pipeline.edges() == (
            ("src1:src:H1", "t1:snk:H1"),
            ("t1:src:H1", "snk1:snk:H1"),
        )
        assert pipeline.edges(pads=False) == (
            ("src1", "t1"),
            ("t1", "snk1"),
        )

    def test_to_graph(self, pipeline):
        """Test to graph."""
        graph = pipeline.to_graph()
        assert isinstance(graph, graphviz.Digraph)

    def test_to_dot(self, pipeline):
        """Test to dot."""
        dot = pipeline.to_dot()
        assert isinstance(dot, str)
        assert dot.split("\n") == [
            "digraph pipeline {",
            "\tgraph [labelloc=t rankdir=LR ranksep=2]",
            '\tnode [fontname="times mono" shape=plaintext]',
            "\tsnk1 [label=<",
            '<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0" '
            'bgcolor="DodgerBlue">',
            '  <TR><TD COLSPAN="3" CELLPADDING="4"><b>snk1</b></TD></TR>',
            '  <TR><TD COLSPAN="3" CELLPADDING="4">NullSink</TD></TR>',
            "  <TR>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="snk1__snk__H1" fixedsize="false" width="18" height="30" '
            'align="left" bgcolor="lightblue">H1</TD></TR>',
            "</TABLE>",
            "",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            "    </TD>",
            "  </TR>",
            "</TABLE>",
            ">]",
            "\tsrc1 [label=<",
            '<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0" '
            'bgcolor="DodgerBlue">',
            '  <TR><TD COLSPAN="3" CELLPADDING="4"><b>src1</b></TD></TR>',
            '  <TR><TD COLSPAN="3" CELLPADDING="4">NullSource</TD></TR>',
            "  <TR>",
            "    <TD>",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="src1__src__H1" fixedsize="false" width="18" height="30" '
            'align="right" bgcolor="MediumAquaMarine">H1</TD></TR>',
            "</TABLE>",
            "",
            "    </TD>",
            "  </TR>",
            "</TABLE>",
            ">]",
            "\tt1 [label=<",
            '<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0" '
            'bgcolor="DodgerBlue">',
            '  <TR><TD COLSPAN="3" CELLPADDING="4"><b>t1</b></TD></TR>',
            '  <TR><TD COLSPAN="3" CELLPADDING="4">CallableTransform</TD></TR>',
            "  <TR>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="t1__snk__H1" fixedsize="false" width="18" height="30" '
            'align="left" bgcolor="lightblue">H1</TD></TR>',
            "</TABLE>",
            "",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="t1__src__H1" fixedsize="false" width="18" height="30" '
            'align="right" bgcolor="MediumAquaMarine">H1</TD></TR>',
            "</TABLE>",
            "",
            "    </TD>",
            "  </TR>",
            "</TABLE>",
            ">]",
            "\tsrc1:src1__src__H1 -> t1:t1__snk__H1",
            "\tt1:t1__src__H1 -> snk1:snk1__snk__H1",
            "}",
            "",
        ]

    def test_to_dot_unlinked(self):
        """Test to graph and output."""
        p = Pipeline()
        p.insert(
            NullSource(
                name="src",
                source_pad_names=("H1",),
            ),
            NullSink(
                name="snk",
                sink_pad_names=("H1",),
            ),
        )
        dot = p.to_dot(label="test")
        assert isinstance(dot, str)
        assert dot.split("\n") == [
            "digraph pipeline {",
            '\tgraph [label=<<font point-size="32"><b>test</b></font>> labelloc=t rankdir=LR ranksep=2]',  # noqa E501
            '\tnode [fontname="times mono" shape=plaintext]',
            "\tsnk [label=<",
            '<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0" '
            'bgcolor="DodgerBlue">',
            '  <TR><TD COLSPAN="3" CELLPADDING="4"><b>snk</b></TD></TR>',
            '  <TR><TD COLSPAN="3" CELLPADDING="4">NullSink</TD></TR>',
            "  <TR>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="snk__snk__H1" fixedsize="false" width="18" height="30" '
            'align="left" bgcolor="tomato">H1</TD></TR>',
            "</TABLE>",
            "",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            "    </TD>",
            "  </TR>",
            "</TABLE>",
            ">]",
            "\tsrc [label=<",
            '<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0" '
            'bgcolor="DodgerBlue">',
            '  <TR><TD COLSPAN="3" CELLPADDING="4"><b>src</b></TD></TR>',
            '  <TR><TD COLSPAN="3" CELLPADDING="4">NullSource</TD></TR>',
            "  <TR>",
            "    <TD>",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="src__src__H1" fixedsize="false" width="18" height="30" '
            'align="right" bgcolor="tomato">H1</TD></TR>',
            "</TABLE>",
            "",
            "    </TD>",
            "  </TR>",
            "</TABLE>",
            ">]",
            "}",
            "",
        ]

    def test_vizualize(self):
        """Test to graph and output."""
        p = Pipeline()
        snk = DequeSink(
            name="snk1",
            sink_pad_names=("H1",),
        )
        p.insert(
            DequeSource(
                name="src1",
                source_pad_names=("H1",),
                # TODO add key formatting helper
                iters={"src1:src:H1": deque([1, 2, 3])},
            ),
            CallableTransform.from_callable(
                name="t1",
                sink_pad_names=("H1",),
                callable=lambda frame: None if frame.data is None else frame.data + 10,
                output_pad_name="H1",
            ),
            snk,
            link_map={
                "t1:snk:H1": "src1:src:H1",
                "snk1:snk:H1": "t1:src:H1",
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "pipeline.svg"
            assert not path.exists()
            p.visualize(path)
            assert path.exists()

    def test_vizualize_err_no_graphviz(self):
        """Test to graph and output Mock the graphviz import to raise
        ModuleNotFoundError by patching sys.modules."""
        p = Pipeline()
        p.insert(
            DequeSource(
                name="src1",
                source_pad_names=("H1",),
            )
        )

        with mock.patch.dict("sys.modules", {"graphviz": None}):
            with pytest.raises(ImportError):
                p.visualize("test.svg")


def test_pipeline_connect_element_to_element():
    """Test basic pipeline.connect() with element-to-element connection."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink

    src = IterSource(name="src", source_pad_names=["H1", "L1"])
    sink = NullSink(name="sink", sink_pad_names=["H1", "L1"])

    pipeline = Pipeline()
    pipeline.connect(src, sink)

    # Verify elements were inserted
    assert "src" in pipeline._registry
    assert "sink" in pipeline._registry

    # Verify graph has connections
    assert len(pipeline.graph) > 0


def test_pipeline_connect_with_groups():
    """Test pipeline.connect() with ElementGroup."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink
    from sgn.groups import group, select

    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1", "V1"])
    sink = NullSink(name="sink", sink_pad_names=["H1", "L1"])

    pipeline = Pipeline()
    sources = group(src1, select(src2, "L1"))
    pipeline.connect(sources, sink)

    # All elements should be inserted
    assert "src1" in pipeline._registry
    assert "src2" in pipeline._registry
    assert "sink" in pipeline._registry

    # Connect using standalone PadSelection as source
    pipeline2 = Pipeline()
    src3 = IterSource(name="src3", source_pad_names=["out"])
    sink2 = NullSink(name="sink2", sink_pad_names=["in"])

    selection = select(src3, "out")
    pipeline2.connect(selection, sink2, link_map={"in": "out"})
    assert "src3" in pipeline2._registry
    assert "sink2" in pipeline2._registry


def test_pipeline_connect_explicit_mapping():
    """Test pipeline.connect() with explicit link_map."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink

    src = IterSource(name="src", source_pad_names=["out"])
    sink = NullSink(name="sink", sink_pad_names=["in"])

    pipeline = Pipeline()
    pipeline.connect(src, sink, link_map={"in": "out"})

    assert "src" in pipeline._registry
    assert "sink" in pipeline._registry

    # Test error cases with invalid pad names in explicit mapping
    src2 = IterSource(name="src2", source_pad_names=["valid"])
    sink2 = NullSink(name="sink2", sink_pad_names=["valid"])

    # Test invalid sink pad name
    with pytest.raises(KeyError, match="sink pad 'invalid_sink' not found"):
        pipeline.connect(src2, sink2, link_map={"invalid_sink": "valid"})

    # Test invalid source pad name
    with pytest.raises(KeyError, match="source pad 'invalid_src' not found"):
        pipeline.connect(src2, sink2, link_map={"valid": "invalid_src"})


def test_pipeline_connect_error_handling():
    """Test pipeline.connect() error handling."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink

    src = IterSource(name="src", source_pad_names=["H1"])
    sink = NullSink(name="sink", sink_pad_names=["L1"])

    pipeline = Pipeline()

    # Passing sink as source - should fail
    with pytest.raises(ValueError, match="no source pads"):
        pipeline.connect(sink, src)

    # Passing source as sink - should fail
    with pytest.raises(ValueError, match="no sink pads"):
        pipeline.connect(src, src)  # Using src as both source and sink


def test_pipeline_connect_mismatched_pads_error():
    """Test error when pad names don't match in ambiguous cases."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink

    # Multiple sources with different names than sinks should be ambiguous
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1"])
    sink1 = NullSink(name="sink1", sink_pad_names=["V1"])
    sink2 = NullSink(name="sink2", sink_pad_names=["X1"])

    from sgn.groups import group

    pipeline = Pipeline()
    sources = group(src1, src2)
    sinks = group(sink1, sink2)

    # No matching pad names and multiple pads on each side - should be ambiguous
    with pytest.raises(
        ValueError, match="unable to determine unambiguous linking strategy"
    ):
        pipeline.connect(sources, sinks)


def test_pipeline_connect_linking_strategies():
    """Test different implicit linking strategies in pipeline.connect()."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink
    from sgn.groups import group

    # N-to-one linking
    src1 = IterSource(name="src1", source_pad_names=["H1"])
    src2 = IterSource(name="src2", source_pad_names=["L1"])
    sink_single = NullSink(name="sink_single", sink_pad_names=["data"])

    pipeline1 = Pipeline()
    sources = group(src1, src2)
    pipeline1.connect(sources, sink_single)
    assert len(pipeline1.graph) > 0

    # One-to-N linking
    src_single = IterSource(name="src_single", source_pad_names=["data"])
    sink1 = NullSink(name="sink1", sink_pad_names=["H1"])
    sink2 = NullSink(name="sink2", sink_pad_names=["L1"])

    pipeline2 = Pipeline()
    sinks = group(sink1, sink2)
    pipeline2.connect(src_single, sinks)
    assert len(pipeline2.graph) > 0


def test_pipeline_connect_partial_matching():
    """Test partial matching strategy - connect all matching pad names."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink
    from sgn.groups import group

    # Sources with pads: H1, L1, V1
    src1 = IterSource(name="src1", source_pad_names=["H1", "L1"])
    src2 = IterSource(name="src2", source_pad_names=["V1"])

    # Sinks with pads: H1, L1, out (only H1 and L1 should match)
    sink1 = NullSink(name="sink1", sink_pad_names=["H1", "L1"])
    sink2 = NullSink(name="sink2", sink_pad_names=["out"])

    pipeline = Pipeline()
    sources = group(src1, src2)  # 3 source pads: H1, L1, V1
    sinks = group(sink1, sink2)  # 3 sink pads: H1, L1, out

    # Should connect H1->H1 and L1->L1, ignoring V1 and out
    pipeline.connect(sources, sinks)

    # Verify connections were made (should have at least the 2 matching connections)
    assert len(pipeline.graph) >= 2

    # Verify elements were inserted
    assert "src1" in pipeline._registry
    assert "src2" in pipeline._registry
    assert "sink1" in pipeline._registry
    assert "sink2" in pipeline._registry


def test_pipeline_connect_ambiguous_linking_error():
    """Test error when linking strategy is ambiguous (no matching pads)."""
    from sgn.sources import IterSource
    from sgn.sinks import NullSink
    from sgn.groups import group

    # multiple sources and multiple sinks with completely different names
    src1 = IterSource(name="src1", source_pad_names=["src_H1", "src_L1"])
    src2 = IterSource(name="src2", source_pad_names=["src_V1"])
    sink1 = NullSink(name="sink1", sink_pad_names=["sink_out1"])
    sink2 = NullSink(name="sink2", sink_pad_names=["sink_out2"])

    pipeline = Pipeline()
    sources = group(src1, src2)  # 3 source pads: src_H1, src_L1, src_V1
    sinks = group(sink1, sink2)  # 2 sink pads: sink_out1, sink_out2

    # No matching pad names - should still be ambiguous
    with pytest.raises(
        ValueError, match="unable to determine unambiguous linking strategy"
    ):
        pipeline.connect(sources, sinks)
