"""Unit tests for the apps module
"""
import pathlib
import tempfile

import pytest

from sgn.apps import Pipeline
from sgn.sinks import FakeSink
from sgn.sources import FakeSrc
from sgn.transforms import FakeTransform

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
        e1 = FakeSrc(name="src1", source_pad_names=("H1",), num_frames=3)
        e2 = FakeSrc(name="src2", source_pad_names=("H1",), num_frames=3)
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
        p.insert(
            FakeSrc(
                name="src1",
                source_pad_names=("H1",),
                num_frames=3,
            ),
            FakeTransform(
                name="trans1",
                sink_pad_names=("H1",),
                source_pad_names=("H1",),
            ),
            FakeSink(
                name="snk1",
                sink_pad_names=("H1",),
            ),
            link_map={
                "trans1:sink:H1": "src1:src:H1",
                "snk1:sink:H1": "trans1:src:H1",
            },
        )

        p.run()

    @pytest.mark.skipif(graphviz is None, reason="graphviz not installed")
    def test_vizualize(self):
        """Test to graph and output"""
        p = Pipeline()
        p.insert(
            FakeSrc(
                name="src1",
                source_pad_names=("H1",),
                num_frames=3,
            ),
            FakeTransform(
                name="trans1",
                sink_pad_names=("H1",),
                source_pad_names=("H1",),
            ),
            FakeSink(
                name="snk1",
                sink_pad_names=("H1",),
            ),
            link_map={
                "trans1:sink:H1": "src1:src:H1",
                "snk1:sink:H1": "trans1:src:H1",
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "pipeline.svg"
            assert not path.exists()
            p.visualize(path)
            assert path.exists()
