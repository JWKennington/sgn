from .. base import *


class FakeHtSrc(SrcElement):
    @initializer
    def __init__(self, **kwargs):
        self.cnt = 0
        kwargs["src_pads"] = [SrcPad(name = "%s:%s:src" % (kwargs["name"], channel), element=self, call = self.new_buffer) for channel in kwargs["channels"]]
        super(FakeHtSrc, self).__init__(**kwargs)
    def new_buffer(self, pad):
        self.cnt += 1
        return Buffer(cnt = self.cnt, name = "buffer: '%s'" % pad.name)

sources_registry = ("FakeHtSrc",)
