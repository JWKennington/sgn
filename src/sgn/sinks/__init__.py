from dataclasses import dataclass
from ..base import *

@dataclass
class FakeSink(SinkElement):
    """
    A fake sink element
    """
    def __post_init__(self):
        self.inbuf = None
        super().__post_init__()
        self.at_eos = {p:False for p in self.sink_pads}

    def get_buffer(self, pad, buf):
        """
	getting the buffer on the pad just modifies the name to show this final
        graph point and the prints it to prove it all works.
        """
        self.inbuf = buf
        self.at_eos[pad] = self.inbuf.EOS
        print ("buffer flow: ", "%s -> '%s'" % (self.inbuf.metadata["name"], pad.name))
    @property
    def EOS(self):
        """
        If buffers on any sink pads are End of Stream (EOS), then mark this whole element as EOS
        """
        return any(self.at_eos.values())

sinks_registry = ("FakeSink",)
