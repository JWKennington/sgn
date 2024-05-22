from dataclasses import dataclass

from ..base import Buffer, TransformElement


@dataclass
class FakeTransform(TransformElement):
    """
    A fake transform element.
    """

    def __post_init__(self):
        self.inbuf = {}
        super().__post_init__()

    def get_buffer(self, pad, buf):
        self.inbuf[pad] = buf

    def transform_buffer(self, pad):
        """
        The transform buffer just update the name to show the graph history.
        Useful for proving it works.  "EOS" is set if any input buffers are at
        EOS.
        """
        EOS = any(b.EOS for b in self.inbuf.values())
        metadata = {
            "cnt:%s" % b.metadata["name"]: b.metadata["cnt"]
            for b in self.inbuf.values()
        }
        metadata["name"] = "%s -> '%s'" % (
            "+".join(b.metadata["name"] for b in self.inbuf.values()),
            pad.name,
        )
        return Buffer(metadata=metadata, EOS=EOS)
