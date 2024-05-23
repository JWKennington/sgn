from dataclasses import dataclass

from ..base import Buffer, SourceElement, SourcePad


@dataclass
class FakeSrc(SourceElement):
    """
    "num_buffers" is required and sets how many buffers will be created before
    setting "EOS"
    """

    num_buffers: int = 0

    def __post_init__(self):
        super().__post_init__()
        self.cnt = {p: 0 for p in self.source_pads}

    def new(self, pad: SourcePad) -> list[Buffer]:
        """
        New buffers are created on "pad" with an instance specific count and a
        name derived from the pad name. "EOS" is set if we have surpassed the
        requested number of buffers.
        """
        self.cnt[pad] += 1
        return [
            Buffer(
                metadata={"cnt": self.cnt, "name": "'%s'" % pad.name},
                EOS=self.cnt[pad] > self.num_buffers,
            )
        ]
