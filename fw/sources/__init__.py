from .. base import *

class FakeHtSrcPad(SrcPad):
    async def __call__(self):
        self.outbuf = self.call()

class FakeHtSrc(SrcElement):
    @initializer
    def __init__(self, **kwargs):
        self.cnt = 0
        kwargs["src_pads"] = [FakeHtSrcPad(name = "%s:src" % kwargs["name"], element=self, call = self.new_buffer)]
        super(FakeHtSrc, self).__init__(**kwargs)
    def new_buffer(self):
        self.cnt += 1
        return Buffer(cnt = self.cnt, name = self.name)

sources_registry = ("FakeHtSrc",)
