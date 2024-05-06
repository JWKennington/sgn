from .. base import *


class FakeSrc(SrcElement):
    @initializer
    def __init__(self, **kwargs):
        """
        A fake source element "channels" are required and will be used to
        create src pads with the name "name:channel:src". "num_buffers" is required and
        sets how many buffers will be created before setting "EOS"
        """
        kwargs["src_pads"] = [SrcPad(name = "%s:%s:src" % (kwargs["name"], channel), element=self, call = self.new_buffer) for channel in kwargs["channels"]]
        self.cnt = {p:0 for p in kwargs["src_pads"]}
        super(FakeSrc, self).__init__(**kwargs)
    def new_buffer(self, pad):
        """
        New buffers are created on "pad" with an instance specific count and a
        name derived from the pad name. "EOS" is set if we have surpassed the requested
        number of buffers.
        """
        self.cnt[pad] += 1
        return Buffer(cnt = self.cnt, name = "'%s'" % pad.name, EOS = self.cnt[pad] > self.num_buffers)

sources_registry = ("FakeSrc",)
