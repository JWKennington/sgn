from .. import base


class FakeOutputSinkPad(base.SinkPad):
    async def __call__(self):
        self.inbuf = self.other.outbuf
        self.call(self.inbuf)

class FakeOutput(base.SinkElement):
    def __init__(self, **kwargs):
        kwargs["sink_pads"] = [FakeOutputSinkPad(name = "%s:sink" % kwargs["name"], element = self, call = self.get_buffer)]
        super(FakeOutput, self).__init__(**kwargs)

    def get_buffer(self, buf):
        self.inbuf = buf
        print (self.inbuf)

sinks_registry = ("FakeOutput",)
