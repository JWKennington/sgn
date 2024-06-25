from dataclasses import dataclass

from .base import Frame, SourceElement, SourcePad


@dataclass
class FakeSrc(SourceElement):
    """
    "num_frames" is required and sets how many Frames will be
    created before returning an EOS
    """

    num_frames: int = 0

    def __post_init__(self):
        super().__post_init__()
        self.cnt = {p: -1 for p in self.source_pads}

    def new(self, pad: SourcePad) -> Frame:
        """
        New Frames are created on "pad" with an instance specific count and a
        name derived from the pad name. EOS is set if we have surpassed the
        requested number of Frames.

        """
        self.cnt[pad] += 1
        return Frame(
            metadata={
                "name": "%s[%d]" % (pad.name, self.cnt[pad]),
            },
            EOS=self.cnt[pad] >= self.num_frames - 1,
        )
