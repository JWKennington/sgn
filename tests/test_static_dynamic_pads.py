"""Test static_pads and allow_dynamic_pads functionality."""

from dataclasses import dataclass
from typing import ClassVar

import pytest

from sgn.base import SinkElement, SourceElement, TransformElement
from sgn.frames import Frame


@dataclass
class StaticOnlyTransform(TransformElement):
    """Transform with static pads only (no dynamic pads allowed)."""

    static_sink_pads: ClassVar[list[str]] = ["input1", "input2"]
    static_source_pads: ClassVar[list[str]] = ["output"]
    allow_dynamic_sink_pads: ClassVar[bool] = False
    allow_dynamic_source_pads: ClassVar[bool] = False

    def pull(self, pad, frame):
        pass

    def new(self, pad):
        return Frame()


@dataclass
class DynamicPadsTransform(TransformElement):
    """Transform with dynamic pads via property."""

    extra_pad_name: str = "extra"

    @property
    def static_sink_pads(self) -> list[str]:  # type: ignore[override]
        return [self.extra_pad_name]

    def pull(self, pad, frame):
        pass

    def new(self, pad):
        return Frame()


@dataclass
class StaticOnlySource(SourceElement):
    """Source with static pads only."""

    static_source_pads: ClassVar[list[str]] = ["output1", "output2"]
    allow_dynamic_source_pads: ClassVar[bool] = False

    def new(self, pad):
        return Frame()


@dataclass
class StaticOnlySink(SinkElement):
    """Sink with static pads only."""

    static_sink_pads: ClassVar[list[str]] = ["input"]
    allow_dynamic_sink_pads: ClassVar[bool] = False

    def pull(self, pad, frame):
        pass


@dataclass
class StaticPlusUserSource(SourceElement):
    """Source with static pads that allows user to add more."""

    static_source_pads: ClassVar[list[str]] = ["monitor"]
    # allow_dynamic_source_pads defaults to True

    def new(self, pad):
        return Frame()


@dataclass
class StaticPlusUserSink(SinkElement):
    """Sink with static pads that allows user to add more."""

    static_sink_pads: ClassVar[list[str]] = ["monitor"]
    # allow_dynamic_sink_pads defaults to True

    def pull(self, pad, frame):
        pass


@dataclass
class StaticSourcePadsTransform(TransformElement):
    """Transform with static source pads."""

    static_source_pads: ClassVar[list[str]] = ["monitor"]

    def pull(self, pad, frame):
        pass

    def new(self, pad):
        return Frame()


class TestStaticOnlyPads:
    """Test static-only pad functionality (allow_dynamic=False)."""

    def test_static_only_transform(self):
        """Test transform with static pads only."""
        elem = StaticOnlyTransform()
        assert elem.sink_pad_names == ["input1", "input2"]
        assert elem.source_pad_names == ["output"]
        assert len(elem.sink_pads) == 2
        assert len(elem.source_pads) == 1

    def test_static_only_source(self):
        """Test source with static pads only."""
        elem = StaticOnlySource()
        assert elem.source_pad_names == ["output1", "output2"]
        assert len(elem.source_pads) == 2

    def test_static_only_sink(self):
        """Test sink with static pads only."""
        elem = StaticOnlySink()
        assert elem.sink_pad_names == ["input"]
        assert len(elem.sink_pads) == 1

    def test_static_only_error_on_user_pads(self):
        """Test error when providing pads with allow_dynamic=False."""
        with pytest.raises(ValueError, match="allow_dynamic_sink_pads=False"):
            StaticOnlyTransform(sink_pad_names=["wrong"])

        with pytest.raises(ValueError, match="allow_dynamic_source_pads=False"):
            StaticOnlyTransform(source_pad_names=["wrong"])

    def test_static_only_source_error_on_user_pads(self):
        """Test error when providing source pads for SourceElement."""
        with pytest.raises(ValueError, match="allow_dynamic_source_pads=False"):
            StaticOnlySource(source_pad_names=["wrong"])

    def test_static_only_sink_error_on_user_pads(self):
        """Test error when providing sink pads for SinkElement."""
        with pytest.raises(ValueError, match="allow_dynamic_sink_pads=False"):
            StaticOnlySink(sink_pad_names=["wrong"])


class TestStaticPlusUserPads:
    """Test static pads combined with user pads (allow_dynamic=True)."""

    def test_static_plus_user_default(self):
        """Test static pads with default user pads."""
        elem = DynamicPadsTransform(
            sink_pad_names=["data"], source_pad_names=["result"]
        )
        assert elem.sink_pad_names == ["data", "extra"]

    def test_static_plus_user_custom(self):
        """Test static pads with custom user pads."""
        elem = DynamicPadsTransform(
            sink_pad_names=["data"],
            source_pad_names=["result"],
            extra_pad_name="custom",
        )
        assert elem.sink_pad_names == ["data", "custom"]

    def test_static_plus_user_dynamic(self):
        """Test that static pads computed from instance attributes."""
        elem1 = DynamicPadsTransform(
            sink_pad_names=["a"],
            source_pad_names=["b"],
            extra_pad_name="x",
        )
        elem2 = DynamicPadsTransform(
            sink_pad_names=["a"],
            source_pad_names=["b"],
            extra_pad_name="y",
        )
        assert elem1.sink_pad_names == ["a", "x"]
        assert elem2.sink_pad_names == ["a", "y"]

    def test_static_source_pads_transform(self):
        """Test static source pads on TransformElement."""
        elem = StaticSourcePadsTransform(
            sink_pad_names=["input"], source_pad_names=["output"]
        )
        assert elem.source_pad_names == ["output", "monitor"]

    def test_static_source_pads_source(self):
        """Test static source pads on SourceElement."""
        elem = StaticPlusUserSource(source_pad_names=["output"])
        assert elem.source_pad_names == ["output", "monitor"]

    def test_static_sink_pads_sink(self):
        """Test static sink pads on SinkElement."""
        elem = StaticPlusUserSink(sink_pad_names=["input"])
        assert elem.sink_pad_names == ["input", "monitor"]


class TestInvalidConfiguration:
    """Test invalid configurations rejected at class definition time."""

    def test_no_static_pads_with_allow_dynamic_false_sink(self):
        """Test allow_dynamic=False without static pads raises error."""
        with pytest.raises(
            TypeError,
            match="allow_dynamic_sink_pads=False but does not define "
            "static_sink_pads",
        ):

            @dataclass
            class BadTransform(TransformElement):
                allow_dynamic_sink_pads: ClassVar[bool] = False

                def pull(self, pad, frame):
                    pass

                def new(self, pad):
                    return Frame()

    def test_no_static_pads_with_allow_dynamic_false_source(self):
        """Test allow_dynamic=False without static pads raises error."""
        with pytest.raises(
            TypeError,
            match="allow_dynamic_source_pads=False but does not define "
            "static_source_pads",
        ):

            @dataclass
            class BadTransform(TransformElement):
                allow_dynamic_source_pads: ClassVar[bool] = False

                def pull(self, pad, frame):
                    pass

                def new(self, pad):
                    return Frame()

    def test_no_static_pads_source_error(self):
        """Test SourceElement error with allow_dynamic=False, no pads."""
        with pytest.raises(
            TypeError,
            match="allow_dynamic_source_pads=False but does not define "
            "static_source_pads",
        ):

            @dataclass
            class BadSource(SourceElement):
                allow_dynamic_source_pads: ClassVar[bool] = False

                def new(self, pad):
                    return Frame()

    def test_no_static_pads_sink_error(self):
        """Test SinkElement error with allow_dynamic=False, no pads."""
        with pytest.raises(
            TypeError,
            match="allow_dynamic_sink_pads=False but does not define "
            "static_sink_pads",
        ):

            @dataclass
            class BadSink(SinkElement):
                allow_dynamic_sink_pads: ClassVar[bool] = False

                def pull(self, pad, frame):
                    pass

    def test_property_with_allow_dynamic_false(self):
        """Test property for static pads with allow_dynamic=False."""

        # This should NOT raise an error - property counts as defining pads
        @dataclass
        class GoodTransformProperty(TransformElement):
            allow_dynamic_sink_pads: ClassVar[bool] = False

            @property
            def static_sink_pads(self) -> list[str]:  # type: ignore[override]
                return ["dynamic"]

            def pull(self, pad, frame):
                pass

            def new(self, pad):
                return Frame()

        # Should be able to instantiate
        elem = GoodTransformProperty(source_pad_names=["output"])
        assert elem.sink_pad_names == ["dynamic"]
