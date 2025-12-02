"""Tests for validator decorators."""

from dataclasses import dataclass

import pytest

from sgn import validator
from sgn.base import SinkElement, TransformElement


@dataclass
class ValidatorTransform(TransformElement):
    """Test transform for validator tests."""

    validation_called: bool = False

    def new(self, pad):
        return None

    def pull(self, pad, frame):
        pass


@dataclass
class ValidatorSink(SinkElement):
    """Test sink for validator tests."""

    validation_called: bool = False

    def pull(self, pad, frame):
        pass


class TestOneToOne:
    """Tests for the one_to_one validator."""

    def test_valid_one_to_one(self):
        """Test that one_to_one passes with exactly one sink and one source pad."""

        class ValidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.one_to_one(self)
                self.validation_called = True

        transform = ValidTransform(
            name="test", sink_pad_names=["in"], source_pad_names=["out"]
        )
        assert transform.validation_called
        assert len(transform.sink_pads) == 1
        assert len(transform.source_pads) == 1

    def test_invalid_multiple_sinks(self):
        """Test that one_to_one fails with multiple sink pads."""

        class InvalidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.one_to_one(self)

        with pytest.raises(
            AssertionError, match="requires exactly one sink and source pad"
        ):
            InvalidTransform(
                name="test", sink_pad_names=["in1", "in2"], source_pad_names=["out"]
            )

    def test_invalid_multiple_sources(self):
        """Test that one_to_one fails with multiple source pads."""

        class InvalidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.one_to_one(self)

        with pytest.raises(
            AssertionError, match="requires exactly one sink and source pad"
        ):
            InvalidTransform(
                name="test", sink_pad_names=["in"], source_pad_names=["out1", "out2"]
            )


class TestOneToMany:
    """Tests for the one_to_many validator."""

    def test_valid_one_to_many(self):
        """Test that one_to_many passes with one sink and multiple source pads."""

        class ValidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.one_to_many(self)
                self.validation_called = True

        transform = ValidTransform(
            name="test", sink_pad_names=["in"], source_pad_names=["out1", "out2"]
        )
        assert transform.validation_called
        assert len(transform.sink_pads) == 1
        assert len(transform.source_pads) == 2

    def test_invalid_multiple_sinks(self):
        """Test that one_to_many fails with multiple sink pads."""

        class InvalidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.one_to_many(self)

        with pytest.raises(AssertionError, match="requires exactly one sink pad"):
            InvalidTransform(
                name="test",
                sink_pad_names=["in1", "in2"],
                source_pad_names=["out1", "out2"],
            )


class TestManyToOne:
    """Tests for the many_to_one validator."""

    def test_valid_many_to_one(self):
        """Test that many_to_one passes with multiple sinks and one source pad."""

        class ValidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.many_to_one(self)
                self.validation_called = True

        transform = ValidTransform(
            name="test", sink_pad_names=["in1", "in2"], source_pad_names=["out"]
        )
        assert transform.validation_called
        assert len(transform.sink_pads) == 2
        assert len(transform.source_pads) == 1

    def test_invalid_multiple_sources(self):
        """Test that many_to_one fails with multiple source pads."""

        class InvalidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.many_to_one(self)

        with pytest.raises(AssertionError, match="requires exactly one source pad"):
            InvalidTransform(
                name="test",
                sink_pad_names=["in1", "in2"],
                source_pad_names=["out1", "out2"],
            )


class TestNumPads:
    """Tests for the num_pads validator."""

    def test_valid_specific_counts(self):
        """Test that num_pads passes with exact sink and source pad counts."""

        class ValidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.num_pads(sink_pads=2, source_pads=3)(self)
                self.validation_called = True

        transform = ValidTransform(
            name="test",
            sink_pad_names=["in1", "in2"],
            source_pad_names=["out1", "out2", "out3"],
        )
        assert transform.validation_called
        assert len(transform.sink_pads) == 2
        assert len(transform.source_pads) == 3

    def test_only_sink_pads_constraint(self):
        """Test that num_pads can validate only sink pads."""

        class ValidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.num_pads(sink_pads=2)(self)
                self.validation_called = True

        transform = ValidTransform(
            name="test",
            sink_pad_names=["in1", "in2"],
            source_pad_names=["out1", "out2", "out3"],  # Any number is fine
        )
        assert transform.validation_called

    def test_only_source_pads_constraint(self):
        """Test that num_pads can validate only source pads."""

        class ValidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.num_pads(source_pads=2)(self)
                self.validation_called = True

        transform = ValidTransform(
            name="test",
            sink_pad_names=["in1", "in2", "in3"],  # Any number is fine
            source_pad_names=["out1", "out2"],
        )
        assert transform.validation_called

    def test_invalid_sink_count(self):
        """Test that num_pads fails with wrong number of sink pads."""

        class InvalidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.num_pads(sink_pads=3)(self)

        with pytest.raises(AssertionError, match="requires exactly 3 sink pad"):
            InvalidTransform(
                name="test", sink_pad_names=["in1", "in2"], source_pad_names=["out"]
            )

    def test_invalid_source_count(self):
        """Test that num_pads fails with wrong number of source pads."""

        class InvalidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.num_pads(source_pads=1)(self)

        with pytest.raises(AssertionError, match="requires exactly 1 source pad"):
            InvalidTransform(
                name="test",
                sink_pad_names=["in"],
                source_pad_names=["out1", "out2"],
            )


class TestPadNamesMatch:
    """Tests for the pad_names_match validator."""

    def test_valid_matching_names(self):
        """Test that pad_names_match passes when source and sink names match."""

        class ValidTransform(ValidatorTransform):
            @validator.pad_names_match
            def __post_init__(self):
                super().__post_init__()
                self.validation_called = True

        transform = ValidTransform(
            name="test",
            sink_pad_names=["H1", "L1"],
            source_pad_names=["H1", "L1"],
        )
        assert transform.validation_called

    def test_valid_different_order(self):
        """Test that pad_names_match passes even if order differs."""

        class ValidTransform(ValidatorTransform):
            @validator.pad_names_match
            def __post_init__(self):
                super().__post_init__()
                self.validation_called = True

        transform = ValidTransform(
            name="test",
            sink_pad_names=["H1", "L1", "V1"],
            source_pad_names=["V1", "H1", "L1"],
        )
        assert transform.validation_called

    def test_invalid_different_names(self):
        """Test that pad_names_match fails when names don't match."""

        class InvalidTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                validator.pad_names_match(self)

        with pytest.raises(
            AssertionError, match="requires source and sink pad names to match"
        ):
            InvalidTransform(
                name="test",
                sink_pad_names=["H1", "L1"],
                source_pad_names=["H1", "V1"],
            )


class TestSinglePad:
    """Tests for the single_pad validator."""

    def test_valid_single_sink(self):
        """Test that single_pad passes with exactly one sink pad."""

        class ValidSink(ValidatorSink):
            def __post_init__(self):
                super().__post_init__()
                validator.single_pad(self)
                self.validation_called = True

        sink = ValidSink(name="test", sink_pad_names=["in"])
        assert sink.validation_called
        assert len(sink.sink_pads) == 1

    def test_invalid_multiple_sinks(self):
        """Test that single_pad fails with multiple sink pads."""

        class InvalidSink(ValidatorSink):
            def __post_init__(self):
                super().__post_init__()
                validator.single_pad(self)

        with pytest.raises(AssertionError, match="requires exactly one sink pad"):
            InvalidSink(name="test", sink_pad_names=["in1", "in2"])


class TestDecoratorUsage:
    """Tests for using validators as decorators on custom validation methods."""

    def test_one_to_one_as_decorator(self):
        """Test that one_to_one works as a decorator on a custom method."""

        class DecoratedTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                self.validate()

            @validator.one_to_one
            def validate(self) -> None:
                self.validation_called = True

        transform = DecoratedTransform(
            name="test", sink_pad_names=["in"], source_pad_names=["out"]
        )
        assert transform.validation_called
        assert len(transform.sink_pads) == 1
        assert len(transform.source_pads) == 1

    def test_num_pads_as_decorator(self):
        """Test that num_pads works as a decorator on a custom method."""

        class DecoratedTransform(ValidatorTransform):
            def __post_init__(self):
                super().__post_init__()
                self.validate()

            @validator.num_pads(sink_pads=2, source_pads=1)
            def validate(self) -> None:
                self.validation_called = True

        transform = DecoratedTransform(
            name="test", sink_pad_names=["in1", "in2"], source_pad_names=["out"]
        )
        assert transform.validation_called
        assert len(transform.sink_pads) == 2
        assert len(transform.source_pads) == 1

    def test_single_pad_as_decorator(self):
        """Test that single_pad works as a decorator on a custom method."""

        class DecoratedSink(ValidatorSink):
            def __post_init__(self):
                super().__post_init__()
                self.validate()

            @validator.single_pad
            def validate(self) -> None:
                self.validation_called = True

        sink = DecoratedSink(name="test", sink_pad_names=["in"])
        assert sink.validation_called
        assert len(sink.sink_pads) == 1
