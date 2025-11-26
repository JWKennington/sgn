"""Validators for simplifying element validation."""

from __future__ import annotations

from functools import wraps
from typing import Callable, TypeAlias, TypeVar, Union, overload

from sgn.base import SinkElement, TransformElement

S = TypeVar("S", bound=SinkElement)
T = TypeVar("T", bound=TransformElement)
U = TypeVar("U", bound=Union[SinkElement, TransformElement])

SMethod: TypeAlias = Callable[[S], None]
TMethod: TypeAlias = Callable[[T], None]


def _make_validator(validate_fn: Callable[[U], None]):
    """Create a validator that can be used as a decorator or called directly.

    Args:
        validate_fn: Function that performs the validation assertion

    Returns:
        A function that works both as a decorator and as a direct validator
    """

    def validator(arg):
        # If arg is callable, it's a method being decorated
        if callable(arg):
            method = arg

            @wraps(method)
            def wrapper(self):
                validate_fn(self)
                return method(self)

            return wrapper

        # Otherwise, it's an instance being validated directly
        validate_fn(arg)
        return None

    return validator


@overload
def one_to_one(arg: TMethod) -> TMethod: ...


@overload
def one_to_one(arg: T) -> None: ...


def one_to_one(arg: TMethod | T) -> TMethod | None:
    """Validator for one-to-one transforms.

    Can be used as a decorator or called directly::

        # As a decorator
        class MyTransform(TransformElement):
            @validator.one_to_one
            def validate(self) -> None:
                pass

        # Called directly
        class MyTransform(TransformElement):
            def __post_init__(self):
                super().__post_init__()
                validator.one_to_one(self)
    """

    def validate(self: T) -> None:
        assert len(self.sink_pads) == 1 and len(self.source_pads) == 1, (
            f"{self.name} requires exactly one sink and source pad, "
            f"got {len(self.sink_pads)} sink pads and "
            f"{len(self.source_pads)} source pads"
        )

    return _make_validator(validate)(arg)


def one_to_many(arg: TMethod | T) -> TMethod | None:
    """Validator for one-to-many transforms.

    Can be used as a decorator or called directly::

        # As a decorator
        class MyTransform(TransformElement):
            @validator.one_to_many
            def validate(self) -> None:
                pass

        # Called directly
        class MyTransform(TransformElement):
            def __post_init__(self):
                super().__post_init__()
                validator.one_to_many(self)
    """

    def validate(self: T) -> None:
        assert len(self.sink_pads) == 1, (
            f"{self.name} requires exactly one sink pad, " f"got {len(self.sink_pads)}"
        )

    return _make_validator(validate)(arg)


@overload
def many_to_one(arg: TMethod) -> TMethod: ...


@overload
def many_to_one(arg: T) -> None: ...


def many_to_one(arg: TMethod | T) -> TMethod | None:
    """Validator for many-to-one transforms.

    Can be used as a decorator or called directly::

        # As a decorator
        class MyTransform(TransformElement):
            @validator.many_to_one
            def validate(self) -> None:
                pass

        # Called directly
        class MyTransform(TransformElement):
            def __post_init__(self):
                super().__post_init__()
                validator.many_to_one(self)
    """

    def validate(self: T) -> None:
        assert len(self.source_pads) == 1, (
            f"{self.name} requires exactly one source pad, "
            f"got {len(self.source_pads)}"
        )

    return _make_validator(validate)(arg)


def num_pads(
    sink_pads: int | None = None, source_pads: int | None = None
) -> Callable[[TMethod], TMethod]:
    """Validator for specific number of sink and source pads.

    Can be used as a decorator or called directly::

        # As a decorator
        class MyTransform(TransformElement):
            @validator.num_pads(sink_pads=2, source_pads=1)
            def validate(self) -> None:
                # Additional custom validation
                pass

        # Called directly
        class MyTransform(TransformElement):
            def __post_init__(self):
                super().__post_init__()
                validator.num_pads(sink_pads=2, source_pads=1)(self)
    """

    def validate(self: T) -> None:
        if sink_pads is not None:
            assert len(self.sink_pads) == sink_pads, (
                f"{self.name} requires exactly {sink_pads} sink pad(s), "
                f"got {len(self.sink_pads)}"
            )
        if source_pads is not None:
            assert len(self.source_pads) == source_pads, (
                f"{self.name} requires exactly {source_pads} source pad(s), "
                f"got {len(self.source_pads)}"
            )

    return _make_validator(validate)  # type: ignore[return-value]


@overload
def pad_names_match(arg: TMethod) -> TMethod: ...


@overload
def pad_names_match(arg: T) -> None: ...


def pad_names_match(arg: TMethod | T) -> TMethod | None:
    """Validator that source and sink pad names match.

    Can be used as a decorator or called directly::

        # As a decorator
        class MyTransform(TransformElement):
            @validator.pad_names_match
            def validate(self) -> None:
                pass

        # Called directly
        class MyTransform(TransformElement):
            def __post_init__(self):
                super().__post_init__()
                validator.pad_names_match(self)
    """

    def validate(self: T) -> None:
        assert set(self.source_pad_names) == set(self.sink_pad_names), (
            f"{self.name} requires source and sink pad names to match. "
            f"Source: {self.source_pad_names}, Sink: {self.sink_pad_names}"
        )

    return _make_validator(validate)(arg)


@overload
def single_pad(arg: SMethod) -> SMethod: ...


@overload
def single_pad(arg: S) -> None: ...


def single_pad(arg: SMethod | S) -> SMethod | None:
    """Validator for single-pad sinks.

    Can be used as a decorator or called directly::

        # As a decorator
        class MySink(SinkElement):
            @validator.single_pad
            def validate(self) -> None:
                pass

        # Called directly
        class MySink(SinkElement):
            def __post_init__(self):
                super().__post_init__()
                validator.single_pad(self)
    """

    def validate(self: S) -> None:
        assert len(self.sink_pads) == 1, (
            f"{self.name} requires exactly one sink pad, " f"got {len(self.sink_pads)}"
        )

    return _make_validator(validate)(arg)
