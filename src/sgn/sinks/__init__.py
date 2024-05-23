from dataclasses import dataclass

from ..base import SinkElement


@dataclass
class FakeSink(SinkElement):
    """
    A fake sink element
    """

    def __post_init__(self):
        super().__post_init__()
        self.at_eos = {p: False for p in self.sink_pads}

    def pull(self, pad, bufs):
        """
        getting the buffer on the pad just modifies the name to show this final
        graph point and the prints it to prove it all works.
        """
        self.at_eos[pad] = bufs[-1].EOS
        print("buffer flow: ", "%s -> '%s'" % (bufs[-1].metadata["name"], pad.name))

    @property
    def EOS(self):
        """
        If buffers on any sink pads are End of Stream (EOS), then mark this
        whole element as EOS
        """
        return any(self.at_eos.values())
