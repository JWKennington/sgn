from .. import base


class FakeOutput(base.SinkElement):
    def __init__(self, **kwargs):
        kwargs["sink_pads"] = [base.SinkPad(name = "%s:%s:sink" % (kwargs["name"], channel), element=self, call = self.get_buffer) for channel in kwargs["channels"]]
        super(FakeOutput, self).__init__(**kwargs)

    def get_buffer(self, pad, buf):
        self.inbuf = buf
        print ("%s -> '%s (on pad %s)'" % (self.inbuf.name, self.name, pad.name))

sinks_registry = ("FakeOutput",)
