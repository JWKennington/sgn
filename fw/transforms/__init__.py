from .. base import *


class FakeTransformSrcPad(SrcPad):
    async def __call__(self):
        self.outbuf = Buffer()

class FakeTransformSinkPad(SinkPad):
    async def __call__(self):
        self.inbuf = self.other.outbuf

class FakeTransform(TransformElement):
    def __init__(self, **kwargs):
        kwargs["sink_pads"] = [FakeTransformSinkPad(name = "%s:sink" % kwargs["name"])]
        kwargs["src_pads"] = [FakeTransformSrcPad(name = "%s:src" % kwargs["name"])]
        super(FakeTransform, self).__init__(**kwargs)


transforms_registry = ("FakeTransform",)
