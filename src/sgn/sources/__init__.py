from dataclasses import dataclass
from .. base import *

@dataclass
class FakeSrc(SrcElement):
    """
    A fake source element "channels" are required and will be used to
    create src pads with the name "name:src:channel". "num_buffers" is required and
    sets how many buffers will be created before setting "EOS"
    """
    num_buffers: int = 0
    channels: list = None
    def __post_init__(self):
        self.src_pads = [SrcPad(name = "%s:src:%s" % (self.name, channel), element=self, call = self.new_buffer) for channel in self.channels]
        self.cnt = {p:0 for p in self.src_pads}
        super().__post_init__()
    def new_buffer(self, pad):
        """
        New buffers are created on "pad" with an instance specific count and a
        name derived from the pad name. "EOS" is set if we have surpassed the requested
        number of buffers.
        """
        self.cnt[pad] += 1
        return Buffer(metadata = {"cnt":self.cnt, "name":"'%s'" % pad.name}, EOS = self.cnt[pad] > self.num_buffers)

sources_registry = ("FakeSrc",)
