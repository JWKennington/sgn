from .. import base


class FakeSink(base.SinkElement):
    def __init__(self, **kwargs):
        """
	A fake sink element that has sink pads given by "channels" named
        "name:channel:sink". "channels" is a required kwarg
        """
        kwargs["sink_pads"] = [base.SinkPad(name = "%s:%s:sink" % (kwargs["name"], channel), element=self, call = self.get_buffer) for channel in kwargs["channels"]]
        super(FakeSink, self).__init__(**kwargs)

    def get_buffer(self, pad, buf):
        """
	getting the buffer on the pad just modifies the name to show this final
        graph point and the prints it to prove it all works.
        """
        self.inbuf = buf
        print ("%s -> '%s (on pad %s)'" % (self.inbuf.name, self.name, pad.name))

sinks_registry = ("FakeSink",)
