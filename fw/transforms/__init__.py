from .. base import *


class FakeTransform(TransformElement):
    @initializer
    def __init__(self, **kwargs):
        """
        A fake transform element with sink pads given by "in_channels" and source pads given by "out_channels". Names are "name:inchannel:sink" and "name:outchannel:src".
        """
        assert "in_channels" in kwargs and "out_channels" in kwargs
        kwargs["src_pads"] = [SrcPad(name = "%s:%s:src" % (kwargs["name"], channel), element = self, call = self.transform_buffer) for channel in self.out_channels]
        kwargs["sink_pads"] = [SinkPad(name = "%s:%s:sink" % (kwargs["name"], channel), element = self, call = self.get_buffer) for channel in self.in_channels]
        super(FakeTransform, self).__init__(**kwargs)
        self.inbuf = {}

    def get_buffer(self, pad, buf):
        self.inbuf[pad] = buf

    def transform_buffer(self, pad):
        """
	The transform buffer just update the name to show the graph history.
        Useful for proving it works...
        """
        return Buffer(**{"cnt:%s" % b.name:b.cnt for b in self.inbuf.values()}, name = "%s -> '%s'" % ("+".join(b.name for b in self.inbuf.values()), pad.name))


transforms_registry = ("FakeTransform",)
