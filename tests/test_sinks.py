"""Test sinks module."""

from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from typing import Iterable, Optional

import pytest

from sgn.base import Frame
from sgn.sinks import DequeSink, NullSink


def test_null():
    sink = NullSink(sink_pad_names=("blah",), verbose=True)
    frame = Frame(data="data")
    sink.pull(sink.sink_pads[0], frame)


class TestDeqSink:
    """Test group for DeqSink class."""

    def test_init(self):
        """Test the FakeSink class constructor."""
        sink = DequeSink(name="snk1", sink_pad_names=("I1", "I2"))
        assert isinstance(sink, DequeSink)
        assert [p.name for p in sink.sink_pads] == ["snk1:snk:I1", "snk1:snk:I2"]
        assert sink.deques == {"I1": deque(), "I2": deque()}
        assert sink.extract_data

    def test_init_err_deques_wrong_number(self):
        """Test init with wrong number of deques."""
        with pytest.raises(AssertionError):
            DequeSink(
                name="snk1",
                sink_pad_names=("I1", "I2"),
                collects={
                    "I1": deque(),
                    "I2": deque(),
                    "I3": deque(),
                },
            )

    def test_init_err_deques_wrong_name(self):
        """Test init with wrong pad name."""
        with pytest.raises(AssertionError):
            DequeSink(
                name="snk1",
                sink_pad_names=("I1", "I2"),
                collects={"I1": deque(), "I3": deque()},
            )

    def test_pull_simple(self):
        """Test pull."""
        sink = DequeSink(name="snk1", sink_pad_names=("I1", "I2"))
        frame = Frame(data="data")
        sink.pull(sink.sink_pads[0], frame)
        sink.internal()
        assert sink.deques["I1"][0] == "data"

    def test_pull_frame(self):
        """Test pull."""
        sink = DequeSink(name="snk1", sink_pad_names=("I1", "I2"), extract_data=False)
        frame = Frame(data="data")
        sink.pull(sink.sink_pads[0], frame)
        sink.internal()
        assert sink.deques["I1"][0] == frame

    def test_pull_frame_buffers(self):
        """Test pull."""

        @dataclass
        class BuffersFrame(Frame):
            buffers: Optional[Iterable] = None

        sink = DequeSink(name="snk1", sink_pad_names=("I1", "I2"), extract_data=True)
        frame = BuffersFrame(data=None, buffers=["buffer1", "buffer2"], is_gap=False)
        sink.pull(sink.sink_pads[0], frame)
        sink.internal()
        assert sink.deques["I1"][0] == frame.buffers

    def test_pull_frame_empty_preserves_deq(self):
        """Test pull."""
        sink = DequeSink(name="snk1", sink_pad_names=("I1", "I2"), extract_data=False)
        assert len(sink.deques["I1"]) == 0

        frame = Frame(data="data")
        sink.pull(sink.sink_pads[0], frame)
        sink.internal()
        assert len(sink.deques["I1"]) == 1
        assert sink.deques["I1"][0] == frame

        frame = Frame(is_gap=True)
        sink.pull(sink.sink_pads[0], frame)
        sink.internal()
        assert len(sink.deques["I1"]) == 1

    def test_pull_frame_skip_empty(self):
        """Test pull."""
        sink = DequeSink(
            name="snk1",
            sink_pad_names=("I1", "I2"),
            extract_data=False,
            skip_empty=True,
        )
        frame = Frame(data=None, is_gap=True)
        sink.pull(sink.sink_pads[0], frame)
        sink.internal()
        assert len(sink.deques["I1"]) == 0

        sink = DequeSink(
            name="snk1",
            sink_pad_names=("I1", "I2"),
            extract_data=False,
            skip_empty=False,
        )
        frame = Frame(data=None)
        sink.pull(sink.sink_pads[0], frame)
        sink.internal()
        assert sink.deques["I1"][0] == frame

    def test_pull_eos(self):
        """Test pull."""
        sink = DequeSink(name="snk1", sink_pad_names=("I1", "I2"))
        frame = Frame(EOS=True)
        sink.pull(sink.sink_pads[0], frame)
        assert sink._at_eos[sink.sink_pads[0]]
