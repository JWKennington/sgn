from .. import base


class FakeOutputSinkPad(base.SinkPad):
    def __call__(self):
        self.outbuf = self.other.outbuf
        print (self.other.outbuf)

class FakeOutput(base.SinkElement):
    def __init__(self, **kwargs):
        kwargs["sink_pads"] = [FakeOutputSinkPad(name = "%s:sink" % kwargs["name"])]
        super(FakeOutput, self).__init__(**kwargs)

sinks_registry = ("FakeOutput",)
