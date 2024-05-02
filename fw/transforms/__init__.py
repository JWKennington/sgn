from .. base import *


class FakeTransform(TransformElement):
    def __init__(self, **kwargs):
        kwargs["src_pads"] = [SrcPad(name = "%s:src" % kwargs["name"], element = self, call = self.transform_buffer)]
        kwargs["sink_pads"] = [SinkPad(name = "%s:sink" % kwargs["name"], element = self, call = self.get_buffer)]
        super(FakeTransform, self).__init__(**kwargs)

    def get_buffer(self, pad, buf):
        self.inbuf = buf

    def transform_buffer(self, pad):
        return Buffer(cnt = self.inbuf.cnt, name = "%s -> '%s'" % (self.inbuf.name, pad.name))


transforms_registry = ("FakeTransform",)
