"""Test sinks module
"""
from sgn.base import Frame
from sgn.sinks import FakeSink


class TestFakeSink:
    """Test group for FakeSink class"""

    def test_init(self):
        """Test the FakeSink class constructor"""
        sink = FakeSink(name='snk1', sink_pad_names=('H1',))
        assert isinstance(sink, FakeSink)
        assert [p.name for p in sink.sink_pads] == ['snk1:sink:H1']

    def test_pull(self, capsys):
        """Test pull"""
        sink = FakeSink(name='snk1', sink_pad_names=('H1',))
        sink.pull(sink.sink_pads[0], Frame(metadata={'name': 'F1'}))
        captured = capsys.readouterr()
        assert captured.out == 'frame flow:  F1 -> snk1:sink:H1\n'
        sink.pull(sink.sink_pads[0], Frame(EOS=True, metadata={'name': 'F2'}))
        captured = capsys.readouterr()
        assert captured.out == 'frame flow:  F2 -> snk1:sink:H1  EOS\n'
