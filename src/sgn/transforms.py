"""Transforms elements and related utilities for the SGN framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional, Sequence

from sgn.base import SinkPad, SourcePad, TransformElement
from sgn.frames import Frame


@dataclass
class InputPull(TransformElement):
    """Input Pull mixin class for Transforms creates a dictionary of inputs and a pull
    method to populate the dictionary.

    The new method remains abstract and must be implemented in the subclass.
    """

    def __post_init__(self):
        self.inputs = {}
        super().__post_init__()

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Pull a frame into the transform element.

        Args:
            pad:
                SinkPad, The sink pad that is receiving the frame
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
            dict[str, Callable], a mapping of output pad names to callables
        depmap:
            dict[str, tuple[str, ...]], mapping of output pad names to input
            combinations
    """

    callmap: dict[str, Callable] = field(default_factory=dict)
    depmap: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def __post_init__(self):
        """Setup callable mappings and name associated source pads."""
        if self.source_pads or self.source_pad_names:
            raise ValueError(
                "CallableTransform does not accept source_pads or "
                "source_pad_names, they are inferred from callmap and namemap"
            )

        # Setup callable maps
        if not self.callmap:
            raise ValueError("CallableTransform requires a callmap")

        # Format callmap keys to ensure name:snk:pad format
        formatted_callmap = {}
        for k, v in self.callmap.items():
            new_key = k
            if not k.startswith(f"{self.name}:src:"):
                new_key = f"{self.name}:src:{k}"
            formatted_callmap[new_key] = v
        self.callmap = formatted_callmap

        # Determine source pad names
        if not self.depmap:
            raise ValueError("CallableTransform requires a depmap")

        # Format namemap keys to ensure name:src:pad format
        formatted_namemap = {}
        for k, v in self.depmap.items():
            new_key = k
            new_val = []
            if not new_key.startswith(f"{self.name}:src:"):
                new_key = f"{self.name}:src:{k}"

            for token in v:
                if token.startswith(f"{self.name}:snk:"):
                    new_val.append(token)
                else:
                    new_val.append(f"{self.name}:snk:{token}")
            new_val = tuple(new_val)
            formatted_namemap[new_key] = new_val

        self.depmap = formatted_namemap

        # Check that callmap and namemap have same set of keys
        if set(self.callmap.keys()) != set(self.depmap.keys()):
            raise ValueError(
                "callmap and namemap must have the same set of keys, "
                f"got {set(self.callmap.keys())} and {set(self.depmap.keys())}"
            )

        # Setup source pad names if needed
        if not self.source_pad_names:
            self.source_pad_names = [
                k.split(":")[-1] for k in sorted(self.depmap.keys())
            ]

        # Setup sink pad names if needed
        if not self.sink_pad_names:
            sink_names = set()
            for v in self.depmap.values():
                sink_names.update(v)
            self.sink_pad_names = [v.split(":")[-1] for v in sorted(sink_names)]

        # Create source pads via parent class
        super().__post_init__()

    def new(self, pad: SourcePad) -> Frame:
        """Apply the callable associated to the pad to the corresponding inputs.

        Args:
            pad:
                SourcePad, The source pad through which the frame is passed

        Returns:
            Frame, the output frame
        """
        # Determine input keys
        input_keys = self.depmap[pad.name]

        # Get callable
        func = self.callmap[pad.name]

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

    @classmethod
    def from_combinations(
        cls,
        name: str,
        combos: Iterable[tuple[tuple[str, ...], Callable, str]],
        sink_pad_names: Optional[Sequence[str]] = None,
        source_pad_names: Optional[Sequence[str]] = None,
    ):
        """Create a CallableTransform from a list of combinations.

        Each combination takes the form:
            (input_keys, func, output_name)

        Args:
            name:
                str, the name of the CallableTransform
            combos:
                Sequence[tuple[tuple[str, ...], Callable, str]], a list of
                combinations to create the CallableTransform, where each
                combination is a tuple of the input keys, the callable, and the
                output name
            sink_pad_names:
                Optional[list[str]], the names of the sink pads (input pads). If not
                specified, inferred from the combos
            source_pad_names:
                Optional[list[str]], the names of the source pads (output pads). If
                not specified, inferred from the combos

        Returns:
            CallableTransform, the created CallableTransform
        """
        callmap = {out: func for _, func, out in combos}
        namemap = {out: inp for inp, _, out in combos}
        return cls(
            name=name,
            callmap=callmap,
            depmap=namemap,
            sink_pad_names=[] if sink_pad_names is None else sink_pad_names,
            source_pad_names=[] if source_pad_names is None else source_pad_names,
        )

    @classmethod
    def from_callable(
        cls,
        name: str,
        callable: Callable,
        output_pad_name: str,
        sink_pad_names: list[str],
    ):
        """Create a CallableTransform from a single callable.

        The callable will be applied to all inputs.

        Args:
            name:
                str, the name of the CallableTransform
            callable:
                Callable, the callable to use for the transform
            output_pad_name:
                str, the name of the output pad
            sink_pad_names:
                list[str], the names of the sink pads (input pads)

        Returns:
            CallableTransform, the created CallableTransform
        """
        return cls(
            name=name,
            sink_pad_names=sink_pad_names,
            callmap={output_pad_name: callable},
            depmap={output_pad_name: tuple(sink_pad_names)},
        )
