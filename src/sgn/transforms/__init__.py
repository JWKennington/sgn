from dataclasses import dataclass

from ..base import Buffer, SourcePad, TransformElement


@dataclass
class FakeTransform(TransformElement):
    """
    A fake transform element.
    """

    def __post_init__(self):
        self.inbufs = {}
        super().__post_init__()

    def pull(self, pad: SourcePad, bufs: list[Buffer]) -> None:
        self.inbufs[pad] = bufs

    def transform(self, pad: SourcePad) -> list[Buffer]:
        """
        The transform buffer just update the name to show the graph history.
        Useful for proving it works.  "EOS" is set if any input buffers are at
        EOS.
        """
        EOS = any(b[-1].EOS for b in self.inbufs.values())
        metadata = {
            "name": "%s -> %s"
            % (
                "+".join(b[-1].metadata["name"] for b in self.inbufs.values()),
                pad.name,
            )
        }
        return [Buffer(metadata=metadata, EOS=EOS)]
