"""Tests for sources module
"""
from sgn.base import Frame
from sgn.sources import FakeSrc


class TestFakeSrc:
    """Test group for FakeSrc class"""

    def test_init(self):
        """Test the FakeSrc class constructor"""
        src = FakeSrc(name='src1', source_pad_names=('H1',), num_frames=3)
        assert isinstance(src, FakeSrc)
        assert [p.name for p in src.source_pads] == ['src1:src:H1']
        assert src.num_frames == 3

    def test_new(self):
        """Test new data method"""
        src = FakeSrc(name='src1', source_pad_names=('H1',), num_frames=3)
        assert src.cnt[src.source_pads[0]] == -1

        # First frame
        frame = src.new(src.source_pads[0])
        assert isinstance(frame, Frame)
        assert frame.metadata['name'] == 'src1:src:H1[0]'
        assert not frame.EOS

        # second frame
        frame = src.new(src.source_pads[0])
        assert not frame.EOS

        # third frame
        frame = src.new(src.source_pads[0])
        assert frame.EOS
