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
        assert snk.deques["snk1:snk:H1"] == deque([13, 12, 11])

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
            '  <TR><TD COLSPAN="3"><b>snk1</b></TD></TR>',
            '  <TR><TD COLSPAN="3">NullSink</TD></TR>',
            "  <TR>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="snk1__snk__H1" fixedsize="true" width="18" height="30" '
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
            '  <TR><TD COLSPAN="3"><b>src1</b></TD></TR>',
            '  <TR><TD COLSPAN="3">NullSource</TD></TR>',
            "  <TR>",
            "    <TD>",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="src1__src__H1" fixedsize="true" width="18" height="30" '
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
            '  <TR><TD COLSPAN="3"><b>t1</b></TD></TR>',
            '  <TR><TD COLSPAN="3">CallableTransform</TD></TR>',
            "  <TR>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="t1__snk__H1" fixedsize="true" width="18" height="30" '
            'align="left" bgcolor="lightblue">H1</TD></TR>',
            "</TABLE>",
            "",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="t1__src__H1" fixedsize="true" width="18" height="30" '
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
            '  <TR><TD COLSPAN="3"><b>snk</b></TD></TR>',
            '  <TR><TD COLSPAN="3">NullSink</TD></TR>',
            "  <TR>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="snk__snk__H1" fixedsize="true" width="18" height="30" '
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
            '  <TR><TD COLSPAN="3"><b>src</b></TD></TR>',
            '  <TR><TD COLSPAN="3">NullSource</TD></TR>',
            "  <TR>",
            "    <TD>",
            "    </TD>",
            "    <TD>-</TD>",
            "    <TD>",
            '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">',
            '<TR><TD PORT="src__src__H1" fixedsize="true" width="18" height="30" '
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
