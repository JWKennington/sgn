from dataclasses import dataclass

from ..base import Buffer, SinkElement, SinkPad


@dataclass
class FakeSink(SinkElement):
    """
    A fake sink element
    """

    def pull(self, pad: SinkPad, bufs: list[Buffer]) -> None:
        """
        getting the buffer on the pad just modifies the name to show this final
        graph point and the prints it to prove it all works.
        """
        if bufs[-1].EOS:
            self.mark_eos(pad)
        print("buffer flow: ", "%s -> '%s'" % (bufs[-1].metadata["name"], pad.name))
