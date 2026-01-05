"""Tests for sgn.compose module - composable elements."""

import pytest

from sgn.apps import Pipeline
from sgn.base import TransformElement, SourceElement, SinkElement
from sgn.compose import (
    Compose,
    ComposedSourceElement,
    ComposedTransformElement,
    ComposedSinkElement,
)
from sgn.sinks import CollectSink, NullSink
from sgn.sources import IterSource
from sgn.transforms import CallableTransform


class TestComposedSourceElement:
    """Tests for ComposedSourceElement."""

    def test_source_only(self):
        """A single source can be wrapped as a composed source."""
        source = IterSource(
            name="src",
            source_pad_names=["out"],
            iters={"out": iter([1, 2, 3])},
        )

        composed = Compose(source).as_source(name="composed_src")

        assert isinstance(composed, ComposedSourceElement)
        assert isinstance(composed, SourceElement)
        assert "out" in composed.srcs
        assert len(composed.source_pads) == 1

    def test_source_plus_transform(self):
        """Source + Transform composes into a source."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2, 3, 4, 5])},
            eos_on_empty={"data": True},
        )

        transform = CallableTransform.from_callable(
            name="double",
            callable=lambda frame: frame.data * 2 if frame.data is not None else None,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        composed = Compose().connect(source, transform).as_source(name="doubled_source")

        assert isinstance(composed, ComposedSourceElement)
        assert "data" in composed.srcs

        # Test in pipeline
        sink = CollectSink(name="sink", sink_pad_names=["data"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        assert list(sink.collects["data"]) == [2, 4, 6, 8, 10]

    def test_source_plus_multiple_transforms(self):
        """Source + multiple transforms compose correctly."""
        source = IterSource(
            name="src",
            source_pad_names=["x"],
            iters={"x": iter([1, 2, 3])},
            eos_on_empty={"x": True},
        )

        add_one = CallableTransform.from_callable(
            name="add_one",
            callable=lambda frame: frame.data + 1 if frame.data is not None else None,
            output_pad_name="x",
            sink_pad_names=["x"],
        )

        square = CallableTransform.from_callable(
            name="square",
            callable=lambda frame: (
                frame.data * frame.data if frame.data is not None else None
            ),
            output_pad_name="x",
            sink_pad_names=["x"],
        )

        # (1+1)^2=4, (2+1)^2=9, (3+1)^2=16
        composed = (
            Compose()
            .connect(source, add_one)
            .connect(add_one, square)
            .as_source(name="processed")
        )

        sink = CollectSink(name="sink", sink_pad_names=["x"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        assert list(sink.collects["x"]) == [4, 9, 16]

    def test_source_composition_with_multi_pad(self):
        """Composed source with multiple pads works correctly."""
        source = IterSource(
            name="src",
            source_pad_names=["H1", "L1"],
            iters={
                "H1": iter([1, 2]),
                "L1": iter([10, 20]),
            },
            eos_on_empty={"H1": True, "L1": True},
        )

        # Transform that doubles each channel
        transform = CallableTransform.from_combinations(
            name="double",
            combos=[
                (
                    ("H1",),
                    lambda frame: frame.data * 2 if frame.data is not None else None,
                    "H1",
                ),
                (
                    ("L1",),
                    lambda frame: frame.data * 2 if frame.data is not None else None,
                    "L1",
                ),
            ],
        )

        composed = Compose().connect(source, transform).as_source(name="doubled")

        assert set(composed.srcs.keys()) == {"H1", "L1"}

        sink = CollectSink(name="sink", sink_pad_names=["H1", "L1"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        assert list(sink.collects["H1"]) == [2, 4]
        assert list(sink.collects["L1"]) == [20, 40]

    def test_invalid_source_composition_with_sink(self):
        """Source composition cannot contain a SinkElement."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2, 3])},
        )

        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        sink = NullSink(name="sink", sink_pad_names=["data"])

        with pytest.raises(TypeError, match="cannot contain SinkElement"):
            (Compose().connect(source, transform).connect(transform, sink).as_source())

    def test_invalid_source_composition_first_not_source(self):
        """Source composition must contain at least one SourceElement."""
        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        with pytest.raises(TypeError, match="requires at least one SourceElement"):
            Compose(transform).as_source()


class TestComposedTransformElement:
    """Tests for ComposedTransformElement."""

    def test_single_transform(self):
        """A single transform can be wrapped."""
        transform = CallableTransform.from_callable(
            name="double",
            callable=lambda frame: frame.data * 2,
            output_pad_name="out",
            sink_pad_names=["in"],
        )

        composed = Compose(transform).as_transform(name="composed_transform")

        assert isinstance(composed, ComposedTransformElement)
        assert isinstance(composed, TransformElement)
        assert "in" in composed.snks
        assert "out" in composed.srcs

    def test_transform_chain(self):
        """Multiple transforms compose into a single transform."""
        t1 = CallableTransform.from_callable(
            name="add_one",
            callable=lambda frame: frame.data + 1 if frame.data is not None else None,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        t2 = CallableTransform.from_callable(
            name="double",
            callable=lambda frame: frame.data * 2 if frame.data is not None else None,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        composed = Compose().connect(t1, t2).as_transform(name="add_then_double")

        assert "data" in composed.snks
        assert "data" in composed.srcs

        # Test in pipeline
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2, 3])},
            eos_on_empty={"data": True},
        )
        sink = CollectSink(name="sink", sink_pad_names=["data"])

        pipeline = Pipeline()
        pipeline.connect(source, composed)
        pipeline.connect(composed, sink)
        pipeline.run()

        # (1+1)*2=4, (2+1)*2=6, (3+1)*2=8
        assert list(sink.collects["data"]) == [4, 6, 8]

    def test_invalid_transform_with_source(self):
        """Transform composition cannot contain SourceElement."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1])},
        )

        with pytest.raises(TypeError, match="can only contain TransformElements"):
            Compose(source).as_transform()

    def test_invalid_transform_with_sink(self):
        """Transform composition cannot contain SinkElement."""
        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data,
            output_pad_name="data",
            sink_pad_names=["data"],
        )
        sink = NullSink(name="sink", sink_pad_names=["data"])

        with pytest.raises(TypeError, match="can only contain TransformElements"):
            Compose().connect(transform, sink).as_transform()


class TestComposedSinkElement:
    """Tests for ComposedSinkElement."""

    def test_sink_only(self):
        """A single sink can be wrapped."""
        sink = NullSink(name="sink", sink_pad_names=["data"])

        composed = Compose(sink).as_sink(name="composed_sink")

        assert isinstance(composed, ComposedSinkElement)
        assert isinstance(composed, SinkElement)
        assert "data" in composed.snks

    def test_transform_plus_sink(self):
        """Transform + Sink composes into a sink."""
        transform = CallableTransform.from_callable(
            name="double",
            callable=lambda frame: frame.data * 2 if frame.data is not None else None,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        collect = CollectSink(name="collect", sink_pad_names=["data"])

        composed = Compose().connect(transform, collect).as_sink(name="doubling_sink")

        assert isinstance(composed, ComposedSinkElement)
        assert "data" in composed.snks

        # Test in pipeline
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2, 3])},
            eos_on_empty={"data": True},
        )

        pipeline = Pipeline()
        pipeline.connect(source, composed)
        pipeline.run()

        # Access internal sink's data
        assert list(collect.collects["data"]) == [2, 4, 6]

    def test_invalid_sink_with_source_first(self):
        """Sink composition cannot contain SourceElement."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1])},
        )

        sink = NullSink(name="sink", sink_pad_names=["data"])

        with pytest.raises(TypeError, match="cannot contain SourceElement"):
            Compose().connect(source, sink).as_sink()

    def test_invalid_sink_not_ending_with_sink(self):
        """Sink composition must contain at least one SinkElement."""
        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        with pytest.raises(TypeError, match="requires at least one SinkElement"):
            Compose(transform).as_sink()


class TestComposeBuilder:
    """Tests for the Compose builder class."""

    def test_explicit_link_map(self):
        """Explicit link_map can override implicit linking."""
        source = IterSource(
            name="src",
            source_pad_names=["out"],
            iters={"out": iter([1, 2, 3])},
            eos_on_empty={"out": True},
        )

        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data * 2 if frame.data is not None else None,
            output_pad_name="result",
            sink_pad_names=["in"],
        )

        # Explicit mapping: connect "out" -> "in"
        composed = (
            Compose()
            .connect(source, transform, link_map={"in": "out"})
            .as_source(name="explicit")
        )

        assert "result" in composed.srcs

        sink = CollectSink(name="sink", sink_pad_names=["result"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        assert list(sink.collects["result"]) == [2, 4, 6]

    def test_compose_preserves_element_identity(self):
        """Internal elements are stored by reference."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1])},
        )

        composed = Compose(source).as_source()

        assert composed.internal_elements[0] is source

    def test_connect_auto_inserts_elements(self):
        """connect() automatically inserts elements not yet in composition."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2, 3])},
            eos_on_empty={"data": True},
        )

        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data * 2 if frame.data is not None else None,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        # Don't call insert() - connect() should handle it
        composed = Compose().connect(source, transform).as_source()

        sink = CollectSink(name="sink", sink_pad_names=["data"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        assert list(sink.collects["data"]) == [2, 4, 6]


class TestImplicitLinking:
    """Tests for implicit linking strategies in composition."""

    def test_exact_match_linking(self):
        """Pads with identical names are linked automatically."""
        source = IterSource(
            name="src",
            source_pad_names=["alpha", "beta"],
            iters={"alpha": iter([1]), "beta": iter([2])},
            eos_on_empty={"alpha": True, "beta": True},
        )

        transform = CallableTransform.from_combinations(
            name="t",
            combos=[
                (
                    ("alpha",),
                    lambda frame: frame.data * 10 if frame.data is not None else None,
                    "alpha",
                ),
                (
                    ("beta",),
                    lambda frame: frame.data * 100 if frame.data is not None else None,
                    "beta",
                ),
            ],
        )

        composed = Compose().connect(source, transform).as_source()

        sink = CollectSink(name="sink", sink_pad_names=["alpha", "beta"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        assert list(sink.collects["alpha"]) == [10]
        assert list(sink.collects["beta"]) == [200]

    def test_n_to_one_linking(self):
        """Multiple source pads can link to single sink pad."""
        source = IterSource(
            name="src",
            source_pad_names=["a", "b"],
            iters={"a": iter([1, 2]), "b": iter([10, 20])},
        )

        # Transform with single sink that receives from multiple sources
        transform = CallableTransform.from_callable(
            name="passthrough",
            callable=lambda frame: frame.data,
            output_pad_name="out",
            sink_pad_names=["in"],
        )

        # With N-to-1, both a and b would try to connect to "in"
        composed = Compose().connect(source, transform).as_source()

        assert "out" in composed.srcs


class TestEOSPropagation:
    """Tests for End-of-Stream propagation in composed elements."""

    def test_composed_source_eos(self):
        """EOS propagates correctly through composed source."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2])},
            eos_on_empty={"data": True},
        )

        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        composed = Compose().connect(source, transform).as_source()

        sink = CollectSink(name="sink", sink_pad_names=["data"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        # Should complete normally with EOS
        assert list(sink.collects["data"]) == [1, 2]

    def test_composed_sink_eos(self):
        """EOS is tracked correctly in composed sink."""
        transform = CallableTransform.from_callable(
            name="t",
            callable=lambda frame: frame.data,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        inner_sink = NullSink(name="inner", sink_pad_names=["data"])

        composed = Compose().connect(transform, inner_sink).as_sink()

        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2])},
            eos_on_empty={"data": True},
        )

        pipeline = Pipeline()
        pipeline.connect(source, composed)
        pipeline.run()

        # Pipeline should complete - check internal sink is at EOS
        assert inner_sink.at_eos


class TestIntegration:
    """Integration tests for composed elements with full pipelines."""

    def test_composed_elements_in_complex_pipeline(self):
        """Composed elements work in complex pipeline configurations."""
        # Create a composed source
        raw_source = IterSource(
            name="raw",
            source_pad_names=["signal"],
            iters={"signal": iter([1, 2, 3, 4, 5])},
            eos_on_empty={"signal": True},
        )

        preprocess = CallableTransform.from_callable(
            name="preprocess",
            callable=lambda frame: frame.data * 2 if frame.data is not None else None,
            output_pad_name="signal",
            sink_pad_names=["signal"],
        )

        composed_source = (
            Compose()
            .connect(raw_source, preprocess)
            .as_source(name="preprocessed_source")
        )

        # Create a composed transform
        t1 = CallableTransform.from_callable(
            name="t1",
            callable=lambda frame: frame.data + 100 if frame.data is not None else None,
            output_pad_name="signal",
            sink_pad_names=["signal"],
        )

        t2 = CallableTransform.from_callable(
            name="t2",
            callable=lambda frame: frame.data // 2 if frame.data is not None else None,
            output_pad_name="signal",
            sink_pad_names=["signal"],
        )

        composed_transform = (
            Compose().connect(t1, t2).as_transform(name="processing_chain")
        )

        # Create a composed sink
        final_transform = CallableTransform.from_callable(
            name="final",
            callable=lambda frame: frame.data * -1 if frame.data is not None else None,
            output_pad_name="signal",
            sink_pad_names=["signal"],
        )

        collector = CollectSink(name="collector", sink_pad_names=["signal"])

        composed_sink = (
            Compose().connect(final_transform, collector).as_sink(name="output_sink")
        )

        # Build pipeline: composed_source -> composed_transform -> composed_sink
        pipeline = Pipeline()
        pipeline.connect(composed_source, composed_transform)
        pipeline.connect(composed_transform, composed_sink)
        pipeline.run()

        # Calculation: raw -> *2 -> +100 -> //2 -> *-1
        # 1 -> 2 -> 102 -> 51 -> -51
        # 2 -> 4 -> 104 -> 52 -> -52
        # 3 -> 6 -> 106 -> 53 -> -53
        # 4 -> 8 -> 108 -> 54 -> -54
        # 5 -> 10 -> 110 -> 55 -> -55
        expected = [-51, -52, -53, -54, -55]
        assert list(collector.collects["signal"]) == expected

    def test_mixed_composed_and_regular_elements(self):
        """Composed elements work alongside regular elements."""
        # Regular source
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([10, 20, 30])},
            eos_on_empty={"data": True},
        )

        # Composed transform
        t1 = CallableTransform.from_callable(
            name="half",
            callable=lambda frame: frame.data // 2 if frame.data is not None else None,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        t2 = CallableTransform.from_callable(
            name="stringify",
            callable=lambda frame: (
                f"value={frame.data}" if frame.data is not None else None
            ),
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        composed_transform = Compose().connect(t1, t2).as_transform()

        # Regular sink
        sink = CollectSink(name="sink", sink_pad_names=["data"])

        pipeline = Pipeline()
        pipeline.connect(source, composed_transform)
        pipeline.connect(composed_transform, sink)
        pipeline.run()

        assert list(sink.collects["data"]) == ["value=5", "value=10", "value=15"]


class TestEdgeCases:
    """Tests for edge cases and validation errors."""

    def test_composed_source_empty_elements(self):
        """ComposedSourceElement with empty elements raises ValueError."""
        with pytest.raises(ValueError, match="requires at least one element"):
            ComposedSourceElement(
                name="empty",
                internal_elements=[],
                internal_links={},
            )

    def test_composed_transform_empty_elements(self):
        """ComposedTransformElement with empty elements raises ValueError."""
        with pytest.raises(ValueError, match="requires at least one element"):
            ComposedTransformElement(
                name="empty",
                internal_elements=[],
                internal_links={},
            )

    def test_composed_sink_empty_elements(self):
        """ComposedSinkElement with empty elements raises ValueError."""
        with pytest.raises(ValueError, match="requires at least one element"):
            ComposedSinkElement(
                name="empty",
                internal_elements=[],
                internal_links={},
            )


class TestNonLinearGraphs:
    """Tests for non-linear graph composition using insert() and connect() API."""

    def test_insert_and_connect_api(self):
        """Test explicit insert() and connect() API for non-linear graphs."""
        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2, 3])},
            eos_on_empty={"data": True},
        )

        transform = CallableTransform.from_callable(
            name="double",
            callable=lambda frame: frame.data * 2 if frame.data is not None else None,
            output_pad_name="data",
            sink_pad_names=["data"],
        )

        # Use explicit insert() and connect()
        composed = (
            Compose()
            .insert(source, transform)
            .connect(source, transform)
            .as_source(name="explicit_compose")
        )

        assert isinstance(composed, ComposedSourceElement)
        assert "data" in composed.srcs

        sink = CollectSink(name="sink", sink_pad_names=["data"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        assert list(sink.collects["data"]) == [2, 4, 6]

    def test_multiple_sources_merging(self):
        """Multiple sources can merge into a multi-pad transform."""
        # Two sources with different pads
        source_a = IterSource(
            name="src_a",
            source_pad_names=["a"],
            iters={"a": iter([1, 2])},
            eos_on_empty={"a": True},
        )

        source_b = IterSource(
            name="src_b",
            source_pad_names=["b"],
            iters={"b": iter([10, 20])},
            eos_on_empty={"b": True},
        )

        # Transform with two inputs and two outputs
        transform = CallableTransform.from_combinations(
            name="proc",
            combos=[
                (
                    ("a",),
                    lambda frame: frame.data * 100 if frame.data is not None else None,
                    "a",
                ),
                (
                    ("b",),
                    lambda frame: frame.data + 1 if frame.data is not None else None,
                    "b",
                ),
            ],
        )

        # Compose using connect() - non-linear structure
        composed = (
            Compose()
            .connect(source_a, transform)
            .connect(source_b, transform)
            .as_source(name="merged_source")
        )

        assert isinstance(composed, ComposedSourceElement)
        assert "a" in composed.srcs
        assert "b" in composed.srcs

        sink = CollectSink(name="sink", sink_pad_names=["a", "b"])
        pipeline = Pipeline()
        pipeline.connect(composed, sink)
        pipeline.run()

        # a: 1*100=100, 2*100=200
        # b: 10+1=11, 20+1=21
        assert list(sink.collects["a"]) == [100, 200]
        assert list(sink.collects["b"]) == [11, 21]

    def test_multiple_sinks_composed(self):
        """Multiple sinks can be composed (fan-out pattern)."""
        transform = CallableTransform.from_combinations(
            name="split",
            combos=[
                (
                    ("data",),
                    lambda frame: frame.data * 2 if frame.data is not None else None,
                    "doubled",
                ),
                (
                    ("data",),
                    lambda frame: frame.data * 3 if frame.data is not None else None,
                    "tripled",
                ),
            ],
        )

        sink1 = CollectSink(name="sink_double", sink_pad_names=["doubled"])
        sink2 = CollectSink(name="sink_triple", sink_pad_names=["tripled"])

        # Compose transform with two sinks using connect()
        composed = (
            Compose()
            .connect(transform, sink1)
            .connect(transform, sink2)
            .as_sink(name="multi_sink")
        )

        assert isinstance(composed, ComposedSinkElement)
        assert "data" in composed.snks

        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1, 2, 3])},
            eos_on_empty={"data": True},
        )

        pipeline = Pipeline()
        pipeline.connect(source, composed)
        pipeline.run()

        # sink1 gets doubled: 2, 4, 6
        # sink2 gets tripled: 3, 6, 9
        assert list(sink1.collects["doubled"]) == [2, 4, 6]
        assert list(sink2.collects["tripled"]) == [3, 6, 9]

    def test_composed_sink_multiple_internal_sinks_eos(self):
        """ComposedSinkElement with multiple sinks reports EOS correctly."""
        sink1 = CollectSink(name="sink1", sink_pad_names=["a"])
        sink2 = CollectSink(name="sink2", sink_pad_names=["b"])

        # Split transform - one input, two outputs
        transform = CallableTransform.from_combinations(
            name="split",
            combos=[
                (
                    ("data",),
                    lambda frame: frame.data if frame.data is not None else None,
                    "a",
                ),
                (
                    ("data",),
                    lambda frame: frame.data if frame.data is not None else None,
                    "b",
                ),
            ],
        )

        composed = (
            Compose()
            .connect(transform, sink1)
            .connect(transform, sink2)
            .as_sink(name="multi_out")
        )

        source = IterSource(
            name="src",
            source_pad_names=["data"],
            iters={"data": iter([1])},
            eos_on_empty={"data": True},
        )

        pipeline = Pipeline()
        pipeline.connect(source, composed)
        pipeline.run()

        # Both internal sinks should be at EOS after pipeline completes
        assert sink1.at_eos
        assert sink2.at_eos
        assert composed.at_eos
