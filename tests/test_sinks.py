"""Test sinks module
"""

from collections import deque

import pytest

from sgn.base import Frame
from sgn.sinks import DequeSink


class TestDeqSink:
    """Test group for DeqSink class"""

    def test_init(self):
        """Test the FakeSink class constructor"""
        sink = DequeSink(name='snk1', sink_pad_names=('I1', 'I2'))
        assert isinstance(sink, DequeSink)
        assert [p.name for p in sink.sink_pads] == ['snk1:sink:I1', 'snk1:sink:I2']
        assert sink.deques == {'snk1:sink:I1': deque(), 'snk1:sink:I2': deque()}
        assert sink.extract_data

    def test_init_err_deques_wrong_number(self):
        """Test init with wrong number of deques"""
        with pytest.raises(ValueError):
            DequeSink(name='snk1', sink_pad_names=('I1', 'I2'), collects={'snk1:sink:I1': deque(), 'snk1:sink:I2': deque(), 'snk1:sink:I3': deque()})

    def test_init_err_deques_wrong_name(self):
        """Test init with wrong pad name"""
        with pytest.raises(ValueError):
            DequeSink(name='snk1', sink_pad_names=('I1', 'I2'), collects={'snk1:sink:I1': deque(), 'snk1:sink:I3': deque()})

    def test_pull_simple(self):
        """Test pull"""
        sink = DequeSink(name='snk1', sink_pad_names=('I1', 'I2'))
        frame = Frame(data='data')
        sink.pull(sink.sink_pads[0], frame)
        assert sink.deques['snk1:sink:I1'][0] == 'data'

    def test_pull_frame(self):
        """Test pull"""
        sink = DequeSink(name='snk1', sink_pad_names=('I1', 'I2'), extract_data=False)
        frame = Frame(data='data')
        sink.pull(sink.sink_pads[0], frame)
        assert sink.deques['snk1:sink:I1'][0] == frame

    def test_pull_frame_empty_preserves_deq(self):
        """Test pull"""
        sink = DequeSink(name='snk1', sink_pad_names=('I1', 'I2'), extract_data=False)
        assert len(sink.deques['snk1:sink:I1']) == 0

        frame = Frame(data='data')
        sink.pull(sink.sink_pads[0], frame)
        assert len(sink.deques['snk1:sink:I1']) == 1
        assert sink.deques['snk1:sink:I1'][0] == frame

        frame = Frame()
        sink.pull(sink.sink_pads[0], frame)
        assert len(sink.deques['snk1:sink:I1']) == 1

    def test_pull_eos(self):
        """Test pull"""
        sink = DequeSink(name='snk1', sink_pad_names=('I1', 'I2'))
        frame = Frame(EOS=True)
        sink.pull(sink.sink_pads[0], frame)
        assert sink._at_eos[sink.sink_pads[0]]
