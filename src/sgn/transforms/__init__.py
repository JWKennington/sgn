from dataclasses import dataclass

from ..base import Frame, SourcePad, TransformElement


@dataclass
class FakeTransform(TransformElement):
    """
    A fake transform element.
    """

    def __post_init__(self):
        self.inputs = {}
        super().__post_init__()

    def pull(self, pad: SourcePad, frame: Frame) -> None:
        self.inputs[pad] = frame

    def transform(self, pad: SourcePad) -> Frame:
        """
        The transform Frame just updates the name value to show the graph history.
        Useful for proving it works.  Sets EOS if any input frames are EOS.
        """
        return Frame(
            metadata={
                "name": "%s -> %s"
                % (
                    "+".join(f.metadata["name"] for f in self.inputs.values()),
                    pad.name,
                )
            },
            EOS=any(frame.EOS for frame in self.inputs.values()),
        )
