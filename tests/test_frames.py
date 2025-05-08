"""Tests for the frames module."""

from dataclasses import dataclass

from sgn.frames import DataSpec, Frame, IterFrame


@dataclass(frozen=True)
class RateDataSpec(DataSpec):
    rate: int


class TestFrame:
    """Tests for the Frame class."""

    def test_init(self):
        """Test the Frame class constructor."""
        f = Frame()
        assert isinstance(f, Frame)
        assert not f.EOS
        assert not f.is_gap
        assert f.data is None


class TestIterFrame:
    """Tests for the IterFrame class."""

    def test_init(self):
        """Test creating an iter frame."""
        frame = IterFrame(data=[1, 2, 3])
        assert isinstance(frame, IterFrame)
        assert frame.data == [1, 2, 3]


class TestDataSpec:
    """Tests for the DataSpec class."""

    def test_update(self):
        """Test update DataSpec properties."""
        spec = RateDataSpec(rate=2048)
        new_spec = spec.update(rate=256)
        assert new_spec.rate == 256
        assert spec != new_spec
