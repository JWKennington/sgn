"""Transforms elements and related utilities for the SGN framework."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from sgn.base import Frame, SourcePad, TransformElement


@dataclass
class InputPull(TransformElement):
    """Input Pull mixin class for Transforms creates a dictionary of inputs and a pull
    method to populate the dictionary.

    The transform method remains abstract and must be implemented in the subclass.
    """

    def __post_init__(self):
        self.inputs = {}
        super().__post_init__()

    def pull(self, pad: SourcePad, frame: Frame) -> None:
        """Pull a frame into the transform element.

        Args:
            pad:
                SourcePad, the pad to pull the frame into.
            frame:
                Frame, the frame to pull into the pad.
        """
        self.inputs[pad.name] = frame


@dataclass
class CallableTransform(InputPull):
    """A transform element that takes a mapping of {(input, combinations) -> callable},
    each of which is mapped to a unique output pad.

    Args:
        callmap:
            dict[tuple[str, ...], Callable], a mapping of input combinations to
            callables
        namemap:
            dict[tuple[str, ...], str], a mapping of input combinations to
            output pad names
    """

    callmap: Optional[dict[tuple[str, ...], Callable]] = None
    namemap: Optional[dict[tuple[str, ...], str]] = None

    def __post_init__(self):
        """Setup callable mappings and name associated source pads."""
        if self.source_pads or self.source_pad_names:
            raise ValueError(
                "CallableTransform does not accept source_pads or "
                "source_pad_names, they are inferred from callmap and namemap"
            )

        # Setup callable maps
        if self.callmap is None:
            raise ValueError("CallableTransform requires a callmap")

        # Format callmap keys to ensure name:sink:pad format
        formatted_callmap = {}
        for k, v in self.callmap.items():
            new_key = []
            for token in k:
                if token.startswith(f"{self.name}:sink:"):
                    new_key.append(token)
                else:
                    new_key.append(f"{self.name}:sink:{token}")
            new_key = tuple(new_key)
            formatted_callmap[new_key] = v
        self.callmap = formatted_callmap

        # Determine source pad names
        if self.namemap is None:
            self.namemap = {
                k: "+".join(sorted(t.split(":")[-1] for t in k))
                for k in sorted(self.callmap.keys())
            }

        # Format namemap keys to ensure name:src:pad format
        formatted_namemap = {}
        for k, v in self.namemap.items():
            new_key = []
            for token in k:
                if token.startswith(f"{self.name}:sink:"):
                    new_key.append(token)
                else:
                    new_key.append(f"{self.name}:sink:{token}")
            new_key = tuple(new_key)
            formatted_namemap[new_key] = v
        self.namemap = formatted_namemap

        # Check that callmap and namemap have same set of keys
        if set(self.callmap.keys()) != set(self.namemap.keys()):
            raise ValueError(
                "callmap and namemap must have the same set of keys, "
                f"got {set(self.callmap.keys())} and {set(self.namemap.keys())}"
            )

        self._namemap_lookup = {
            f"{self.name}:src:{v}": k for k, v in self.namemap.items()
        }

        # Setup source pads
        self.source_pad_names = list(self.namemap.values())

        # Create source pads via parent class
        super().__post_init__()

    def transform(self, pad: SourcePad) -> Frame:
        """Apply the callable associated to the pad to the corresponding inputs.

        Args:
            pad:
                SourcePad, the pad to transform

        Returns:
            Frame, the output frame
        """
        # Determine input keys
        input_keys = self._namemap_lookup[pad.name]

        # Get callable
        assert isinstance(self.callmap, dict)
        func = self.callmap[input_keys]

        # Get inputs
        input_args = tuple(
            self.inputs[k] for k in input_keys
        )  # same order as input_keys

        # Apply callable
        res = func(*input_args)

        return Frame(
            # TODO: generalize this to choose any v. all behavior
            EOS=any(frame.EOS for frame in self.inputs.values()),
            data=res,
        )

    @staticmethod
    def from_combinations(
        name: str,
        sink_pad_names: list[str],
        combos: Iterable[tuple[tuple[str, ...], Callable, str]],
    ):
        """Create a CallableTransform from a list of combinations where each combination
        is.

            (input_keys, func, output_name)

        Args:
            name:
                str, the name of the CallableTransform
            sink_pad_names:
                list[str], the names of the sink pads (input pads)
            combos:
                Iterable[tuple[tuple[str, ...], Callable, str]], a list of
                combinations to create the CallableTransform, where each
                combination is a tuple of the input keys, the callable, and the
                output name

        Returns:
            CallableTransform, the created CallableTransform
        """
        callmap = {k: f for k, f, _ in combos}
        namemap = {k: n for k, _, n in combos}
        return CallableTransform(
            name=name, sink_pad_names=sink_pad_names, callmap=callmap, namemap=namemap
        )

    @staticmethod
    def from_callable(
        name: str,
        sink_pad_names: list[str],
        callable: Callable,
        output_name: Optional[str] = None,
    ):
        """Create a CallableTransform from a single callable that will be applied to all
        inputs.

        Args:
            name:
                str, the name of the CallableTransform
            sink_pad_names:
                list[str], the names of the sink pads (input pads)
            callable:
                Callable, the callable to use for the transform

        Returns:
            CallableTransform, the created CallableTransform
        """
        return CallableTransform(
            name=name,
            sink_pad_names=sink_pad_names,
            callmap={tuple(sink_pad_names): callable},
            namemap=(
                None if output_name is None else {tuple(sink_pad_names): output_name}
            ),
        )
