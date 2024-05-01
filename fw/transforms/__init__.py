from .. base import *


class FakeTransformSrcPad(SrcPad):
    async def __call__(self):
        self.outbuf = self.call()

class FakeTransformSinkPad(SinkPad):
    async def __call__(self):
        self.inbuf = self.other.outbuf
        self.call(self.inbuf)

class FakeTransform(TransformElement):
    def __init__(self, **kwargs):
        kwargs["src_pads"] = [FakeTransformSrcPad(name = "%s:src" % kwargs["name"], element = self, call = self.transform_buffer)]
        kwargs["sink_pads"] = [FakeTransformSinkPad(name = "%s:sink" % kwargs["name"], element = self, call = self.get_buffer)]
        super(FakeTransform, self).__init__(**kwargs)

    def get_buffer(self, buf):
        self.inbuf = buf

    def transform_buffer(self):
        return Buffer(cnt = self.inbuf.cnt, name = self.name)


transforms_registry = ("FakeTransform",)
