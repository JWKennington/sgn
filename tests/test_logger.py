"""Unit tests for the logger module."""

import logging
import os
from unittest import mock

import pytest

from sgn.logger import SGN_LOG_LEVELS, configure_sgn_logging


def test_set_default_level_via_env_var():
    """Test setting the log level via an environment variable."""
    with mock.patch.dict(os.environ, {"SGNLOGLEVEL": "DEBUG"}):
        assert os.environ["SGNLOGLEVEL"] == "DEBUG"

        configure_sgn_logging()
        logger = logging.getLogger("sgn")
        assert isinstance(logger, logging.Logger)
        assert logger.level == SGN_LOG_LEVELS["DEBUG"]


def test_set_scoped_level_via_env_var():
    """Test setting the element scoped log level via an environment variable."""
    with mock.patch.dict(os.environ, {"SGNLOGLEVEL": "myelement:DEBUG"}):
        assert os.environ["SGNLOGLEVEL"] == "myelement:DEBUG"

        configure_sgn_logging()
        logger = logging.getLogger("sgn").getChild("myelement")
        assert isinstance(logger, logging.Logger)
        assert logger.level == SGN_LOG_LEVELS["DEBUG"]


def test_err_default_invalid_level():
    """Test setting the log level via an environment variable."""
    with mock.patch.dict(os.environ, {"SGNLOGLEVEL": "INVALID"}):
        assert os.environ["SGNLOGLEVEL"] == "INVALID"

        with pytest.raises(ValueError):
            configure_sgn_logging()
