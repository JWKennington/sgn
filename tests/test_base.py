"""Unit tests for the base module."""

import asyncio
import random
import os
from dataclasses import dataclass
from logging import Logger
from unittest import mock

import pytest

from sgn.base import (
    SGN_LOG_LEVELS,
    ElementLike,
    Frame,
    SinkPad,
    SourcePad,
    UniqueID,
    get_sgn_logger,
)
from sgn.frames import DataSpec


def asyncio_run(coro):
    """Run an asyncio coroutine."""
    return asyncio.get_event_loop().run_until_complete(coro)


@dataclass(frozen=True)
class RateDataSpec(DataSpec):
    rate: int


class TestUniqueID:
    """Test group for the UniqueID class."""

    def test_init(self):
        """Test the UniqueID class constructor."""
        ui = UniqueID()
        assert ui._id
        assert ui.name == ui._id

        ui = UniqueID(name="test")
        assert ui.name == "test"

    def test_hash(self):
        """Test the __hash__ method."""
        ui = UniqueID()
        assert hash(ui) == hash(ui._id)

    def test_eq(self):
        """Test the __eq__ method."""
        ui1 = UniqueID()
        ui2 = UniqueID()
        assert ui1 == ui1
        assert ui1 != ui2


class TestSourcePad:
    """Test group for SourcePad class."""

    def test_init(self):
        """Test the SourcePad class constructor."""
        sp = SourcePad(element=None, call=None, output=None)
        assert isinstance(sp, SourcePad)
        assert sp.output is None

    def test_call(self):
        """Test the __call__ method."""

        def dummy_func(pad):
            return Frame()

        sp = SourcePad(name="testsrc", element=None, call=dummy_func, output=None)

        # Run
        asyncio_run(sp())

        assert isinstance(sp.output, Frame)


class TestSinkPad:
    """Test group for SinkPad class."""

    def test_init(self):
        """Test the SinkPad class constructor."""
        sp = SinkPad(element=None, call=None, input=None)
        assert isinstance(sp, SinkPad)
        assert sp.input is None

    def test_link(self):
        """Test the link method."""
        s1 = SourcePad(name="testsrc", element=None, call=None, output=None)
        s2 = SinkPad(element="testsink", call=None, input=None)

        # Catch error for linking wrong item
        with pytest.raises(AssertionError):
            s2.link(None)

        assert s2.other is None
        res = s2.link(s1)
        assert s2.other == s1
        assert res == {s2: set([s1])}

    def test_call(self):
        """Test the __call__ method."""

        def dummy_src(pad):
            spec = RateDataSpec(rate=random.randint(1, 2048))
            return Frame(spec=spec)

        def dummy_snk(pad, frame):
            return None

        p1 = SourcePad(name="testsrc", element=None, call=dummy_src, output=None)
        p2 = SinkPad(name="testsink", element=None, call=dummy_snk, input=None)

        # Try running before linking (bad)
        with pytest.raises(AssertionError):
            asyncio_run(p2())

        # Link
        p2.link(p1)

        # Run wrong order
        with pytest.raises(AssertionError):
            asyncio_run(p2())

        # Run correct order
        asyncio_run(p1())
        asyncio_run(p2())
        assert p2.input is not None

        # Run again, data specification will be different
        asyncio_run(p1())
        with pytest.raises(ValueError):
            asyncio_run(p2())


class TestElementLike:
    """Test group for element like class."""

    def test_init(self):
        """Test the element like class constructor."""
        el = ElementLike()
        assert isinstance(el, ElementLike)
        assert el.source_pads == []
        assert el.sink_pads == []
        assert el.graph == {}

    def test_source_pad_dict(self):
        """Test the source_pad_dict method."""
        src = SourcePad(name="testsrc", element=None, call=None, output=None)
        el = ElementLike(source_pads=[src])
        assert el.source_pad_dict == {"testsrc": src}

    def test_sink_pad_dict(self):
        """Test the sink_pad_dict method."""
        snk = SinkPad(name="testsink", element=None, call=None, input=None)
        el = ElementLike(sink_pads=[snk])
        assert el.sink_pad_dict == {"testsink": snk}

    def test_pad_list(self):
        """Test the pad_list method."""
        src = SourcePad(name="testsrc", element=None, call=None, output=None)
        snk = SinkPad(name="testsink", element=None, call=None, input=None)
        el = ElementLike(source_pads=[src], sink_pads=[snk])
        # Pad list will have an automatically generated internal pad as the
        # last entry
        assert len(el.pad_list) == 3 and el.pad_list[:2] == [src, snk]


class TestLogging:
    """Test group for logging functions."""

    def test_set_default_level_via_env_var(self):
        """Test setting the log level via an environment variable."""
        with mock.patch.dict(os.environ, {"SGNLOGLEVEL": "DEBUG"}):
            assert os.environ["SGNLOGLEVEL"] == "DEBUG"

            logger = get_sgn_logger("sample", SGN_LOG_LEVELS)
            assert isinstance(logger, Logger)

    def test_set_scoped_level_via_env_var(self):
        """Test setting the element scoped log level via an environment variable."""
        with mock.patch.dict(os.environ, {"SGNLOGLEVEL": "myelement:DEBUG"}):
            assert os.environ["SGNLOGLEVEL"] == "myelement:DEBUG"

            logger = get_sgn_logger("sample", SGN_LOG_LEVELS).getChild("myelement")
            assert isinstance(logger, Logger)

    def test_err_default_invalid_level(self):
        """Test setting the log level via an environment variable."""
        with mock.patch.dict(os.environ, {"SGNLOGLEVEL": "INVALID"}):
            assert os.environ["SGNLOGLEVEL"] == "INVALID"

            with pytest.raises(ValueError):
                get_sgn_logger("sample", SGN_LOG_LEVELS)
