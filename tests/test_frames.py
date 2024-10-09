"""Tests for the frames module."""

from sgn import IterFrame


class TestIterFrame:
    """Tests for the IterFrame class."""

    def test_init(self):
        """Test creating an iter frame."""
        frame = IterFrame(data=[1, 2, 3])
        assert isinstance(frame, IterFrame)
        assert frame.data == [1, 2, 3]
