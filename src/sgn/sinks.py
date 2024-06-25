from dataclasses import dataclass

from .base import Frame, SinkElement, SinkPad


@dataclass
class FakeSink(SinkElement):
    """
    A fake sink element
    """

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """
        getting the frame on the pad just modifies the name to show this final
        graph point and the prints it to prove it all works.
        """
        if frame.EOS:
            self.mark_eos(pad)
        msg = "frame flow:  %s -> %s" % (frame.metadata["name"], pad.name)
        if self.at_eos:
            msg += "  EOS"
        print(msg)
