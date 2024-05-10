from dataclasses import dataclass
from .. base import *

@dataclass
class FakeTransform(TransformElement):
    """
    A fake transform element with sink pads given by "in_channels" and source pads given by "out_channels". Names are "name:sink:inchannel" and "name:src:outchannel".
    """
    in_channels: list = None
    out_channels: list = None

    def __post_init__(self):
        self.src_pads = [SrcPad(name = "%s:src:%s" % (self.name, channel), element = self, call = self.transform_buffer) for channel in self.out_channels]
        self.sink_pads = [SinkPad(name = "%s:sink:%s" % (self.name, channel), element = self, call = self.get_buffer) for channel in self.in_channels]
        self.inbuf = {}
        super().__post_init__()

    def get_buffer(self, pad, buf):
        self.inbuf[pad] = buf

    def transform_buffer(self, pad):
        """
	The transform buffer just update the name to show the graph history.
        Useful for proving it works.  "EOS" is set if any input buffers are at EOS.
        """
        EOS = any(b.EOS for b in self.inbuf.values())
        metadata = {"cnt:%s" % b.metadata['name']:b.metadata['cnt'] for b in self.inbuf.values()}
        metadata["name"] = "%s -> '%s'" % ("+".join(b.metadata["name"] for b in self.inbuf.values()), pad.name)
        return Buffer(metadata = metadata, EOS = EOS)

transforms_registry = ("FakeTransform",)
