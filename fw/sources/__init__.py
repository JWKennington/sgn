from .. base import *


class FakeSrc(SrcElement):
    @initializer
    def __init__(self, **kwargs):
        """
        A fake source element "channels" are required and will be used to create src pads with the name "name:channel:src"
        """
        self.cnt = 0
        kwargs["src_pads"] = [SrcPad(name = "%s:%s:src" % (kwargs["name"], channel), element=self, call = self.new_buffer) for channel in kwargs["channels"]]
        super(FakeSrc, self).__init__(**kwargs)
    def new_buffer(self, pad):
        """
        New buffers are created on "pad" with an instance specific count and a name derived from the pad name
        """
        self.cnt += 1
        return Buffer(cnt = self.cnt, name = "buffer: '%s'" % pad.name)

sources_registry = ("FakeSrc",)
