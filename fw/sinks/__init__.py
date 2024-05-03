from ..base import *


class FakeSink(SinkElement):
    @initializer
    def __init__(self, **kwargs):
        """
	A fake sink element that has sink pads given by "channels" named
        "name:channel:sink". "channels" is a required kwarg
        """
        kwargs["sink_pads"] = [SinkPad(name = "%s:%s:sink" % (kwargs["name"], channel), element=self, call = self.get_buffer) for channel in kwargs["channels"]]
        super(FakeSink, self).__init__(**kwargs)

    def get_buffer(self, pad, buf):
        """
	getting the buffer on the pad just modifies the name to show this final
        graph point and the prints it to prove it all works.
        """
        self.inbuf = buf
        print ("buffer flow: ", "%s -> '%s'" % (self.inbuf.name, pad.name))

sinks_registry = ("FakeSink",)
