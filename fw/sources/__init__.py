from .. base import *

class FakeHtSrcPad(SrcPad):
    def __call__(self):
        self.outbuf = Buffer()

class FakeHtSinkPad(SinkPad):
    pass

class FakeHtSrc(SrcElement):
    @initializer
    def __init__(self, **kwargs):
        kwargs["src_pads"] = [FakeHtSrcPad(name = "%s:src" % kwargs["name"])]
        super(FakeHtSrc, self).__init__(**kwargs)

sources_registry = ("FakeHtSrc",)
