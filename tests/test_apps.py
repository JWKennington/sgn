"""Unit tests for the apps module
"""
import pathlib
import tempfile
from collections import deque
from unittest import mock

import pytest

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
    """Test group for Pipeline class"""

    def test_init(self):
        """Test Pipeline.__init__"""
        p = Pipeline()
        assert isinstance(p, Pipeline)
        assert p.graph == {}
        assert p._registry == {}
        assert p.sinks == {}

    def test_element_validation(self):
        """Test element validation"""
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
        """Test execute graphs"""
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
                output_name="H1",
            ),
            snk
            ,
            link_map={
                "t1:sink:H1": "src1:src:H1",
                "snk1:sink:H1": "t1:src:H1",
            },
        )

        p.run()
        assert snk.deques["snk1:sink:H1"] == deque([13, 12, 11])

    def test_vizualize(self):
        """Test to graph and output"""
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
                output_name="H1",
            ),
            snk
            ,
            link_map={
                "t1:sink:H1": "src1:src:H1",
                "snk1:sink:H1": "t1:src:H1",
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "pipeline.svg"
            assert not path.exists()
            p.visualize(path)
            assert path.exists()

    def test_vizualize_err_no_graphviz(self):
        """Test to graph and output
        Mock the graphviz import to raise ModuleNotFoundError by patching sys.modules
        """
        p = Pipeline()
        p.insert(
            DequeSource(
                name="src1",
                source_pad_names=("H1",),
            )
        )

        with mock.patch.dict("sys.modules", {"graphviz": None}):
            with pytest.raises(ImportError):
                p.visualize('test.svg')
