"""Logging utilities and configuration for SGN."""

import logging
import os

# SGN-specific log levels
SGN_LOG_LEVELS = {
    "MEMPROF": 5,  # custom memory profiling level
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# Environment variable for configuring log levels
SGN_LOG_LEVEL_VAR = "SGNLOGLEVEL"


def setup_custom_levels():
    """Set up SGN's custom logging levels.

    This function registers custom logging levels like MEMPROF with Python's
    logging system. This function is idempotent - safe to call multiple times.
    """
    _add_logging_level("MEMPROF", SGN_LOG_LEVELS["MEMPROF"], "memprofile")


def configure_sgn_logging():
    """Configure SGN logging with handlers and environment-based levels.

    Environment Variables:
        SGNLOGLEVEL: Space-separated list of logger configurations in format:
                    - "LEVEL" (applies to the main logger)
                    - "logger_name:LEVEL" (applies to specific child logger)

    Examples:
        SGNLOGLEVEL="DEBUG" -> Sets main logger to DEBUG level
        SGNLOGLEVEL="pipeline:INFO subprocess:WARNING" -> Sets specific loggers
    """
    # Set up the root SGN logger
    sgn_logger = logging.getLogger("sgn")

    # Add StreamHandler if not already present
    if not sgn_logger.handlers:
        handler = logging.StreamHandler()
        sgn_logger.addHandler(handler)

    # Parse environment variable for log level configuration
    if SGN_LOG_LEVEL_VAR in os.environ:
        config_levels = _parse_log_level_config(
            os.environ[SGN_LOG_LEVEL_VAR], "sgn", SGN_LOG_LEVELS
        )
        _apply_log_levels(sgn_logger, "sgn", config_levels, SGN_LOG_LEVELS)


def _parse_log_level_config(
    config_str: str, default_name: str, valid_levels: dict[str, int]
) -> dict[str, str]:
    """Parse log level configuration string from environment variable.

    Args:
        config_str: Space-separated configuration string
        default_name: Default logger name to use when only level is specified
        valid_levels: Dictionary of valid log level names

    Returns:
        Dictionary mapping logger names to log level names

    Raises:
        ValueError: If an invalid log level is specified
    """

    def parse_single_config(config_item: str) -> tuple[str, str]:
        parts = config_item.split(":")
        if len(parts) == 1:
            # Just a level specified, applies to default logger
            logger_name, level = default_name, parts[0]
        else:
            # Logger name and level specified
            logger_name, level = parts[0], parts[1]

        if level not in valid_levels:
            raise ValueError(
                f"Invalid log level: {level}, choose from {list(valid_levels.keys())}"
            )

        return logger_name, level

    return dict(parse_single_config(item) for item in config_str.split())


def _add_logging_level(level_name, level_num, method_name=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    Based on: https://stackoverflow.com/a/35804945

    Args:
        level_name: Name of the logging level (e.g., 'MEMPROF')
        level_num: Numeric value for the level
        method_name: Name of the method to add (defaults to level_name.lower())
    """
    if not method_name:
        method_name = level_name.lower()  # pragma: no cover

    # Check if the level is already registered (idempotency check)
    if hasattr(logging.getLoggerClass(), method_name):
        return  # Already configured, skip

    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)  # pragma: no cover

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)


def _apply_log_levels(
    logger: logging.Logger,
    main_name: str,
    config: dict[str, str],
    levels: dict[str, int],
):
    """Apply log level configuration to logger and its children.

    Args:
        logger: The main logger instance
        main_name: Name of the main logger
        config: Dictionary mapping logger names to level names
        levels: Dictionary mapping level names to level values
    """
    for logger_name, level_name in config.items():
        level_value = levels[level_name]

        if logger_name == main_name:
            logger.setLevel(level_value)
        else:
            child_logger = logger.getChild(logger_name)
            child_logger.setLevel(level_value)
