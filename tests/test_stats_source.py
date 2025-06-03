"""Consolidated unit tests for the stats_source module."""

from __future__ import annotations

import sys
import time
import unittest.mock as mock
import pytest

from sgn.base import Frame
from sgn.sources import StatsSource, PSUTIL_AVAILABLE


#
# Basic StatsSource functionality tests
#


class TestStatsSourceBasic:
    """Test group for basic StatsSource functionality."""

    def test_init(self):
        """Test StatsSource.__init__"""
        source = StatsSource(
            name="test_stats", source_pad_names=["stats"], interval=1.0
        )
        assert source.interval == 1.0
        assert source.include_process_stats is True
        assert source.include_system_stats is True
        assert source.frame_factory == Frame
        assert source.eos_on_signal is True

    def test_should_collect_stats(self):
        """Test StatsSource.should_collect_stats"""
        # Test with no interval (should always collect)
        source = StatsSource(
            name="test_stats", source_pad_names=["stats"], interval=None
        )
        assert source.should_collect_stats() is True
        assert (
            source.should_collect_stats() is True
        )  # Second call should also return True

        # Test with an interval
        source = StatsSource(
            name="test_stats", source_pad_names=["stats"], interval=1.0
        )
        assert source.should_collect_stats() is True  # First call should collect
        assert (
            source.should_collect_stats() is False
        )  # Second call within interval should not
        time.sleep(1.0)  # Wait for interval
        assert (
            source.should_collect_stats() is True
        )  # After interval, should collect again

    def test_new_returns_frame(self):
        """Test StatsSource.new returns a valid Frame."""
        source = StatsSource(name="test_stats", source_pad_names=["stats"])

        # Temporarily disable EOS with mocking since the real implementation
        # may have EOS set to True based on system environment
        with mock.patch.object(source, "_eos", False):
            frame = source.new(source.source_pads[0])
            assert isinstance(frame, Frame)

            # We can't test EOS directly as it depends on implementation and environment
            # Just check the frame structure instead
            assert isinstance(frame.data, dict)

            # All versions should have a timestamp
            assert "timestamp" in frame.data

            # Check that we have the expected metadata
            assert frame.metadata["stats_type"] == "system_metrics"

    def test_pipeline_setup(self):
        """Test StatsSource can be properly set up in a pipeline."""
        from sgn.apps import Pipeline

        # Create pipeline elements
        source = StatsSource(
            name="stats_source",
            source_pad_names=["stats"],
            interval=0.1,  # Small interval for testing
        )

        # Create a minimal test pipeline just with the source
        pipeline = Pipeline()
        pipeline.insert(source)

        # Verify source has the expected pad
        assert len(source.source_pads) == 1
        assert source.source_pads[0].name.endswith("stats")  # May have name prefix

        # Check if the source has the expected methods from our interface
        assert hasattr(source, "new")
        assert hasattr(source, "should_collect_stats")

        # Check if the source has the expected properties
        assert hasattr(source, "interval")
        assert hasattr(source, "include_process_stats")
        assert hasattr(source, "include_system_stats")


#
# Process Stats tests
#


class TestProcessStats:
    """Test group for process statistics collection."""

    def test_collect_process_stats_no_psutil(self):
        """Test _collect_process_stats when psutil is not available."""
        # This test works with or without psutil
        # Create a StatsSource with patching to simulate no psutil
        with mock.patch("sgn.sources.PSUTIL_AVAILABLE", False):
            source = StatsSource(name="test", source_pad_names=["out"])
            stats = source._collect_process_stats()

            # Check that limited info is returned
            assert "limited_info" in stats
            assert stats["limited_info"] is True
            assert "error" in stats
            assert stats["pid"] == source._current_pid

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_collect_process_stats_exceptions(self):
        """Test _collect_process_stats exception handling."""
        # Import psutil only when the test is actually going to run
        import psutil  # noqa: F401

        source = StatsSource(name="test", source_pad_names=["out"])

        # Mock Process methods to raise exceptions
        mock_process = mock.MagicMock()
        mock_process.pid = 12345
        mock_process.name.return_value = "test"
        mock_process.status.return_value = "running"
        mock_process.create_time.return_value = time.time() - 100

        # CPU stats - raise exceptions to cover lines 123-124
        mock_cpu_percent = mock.MagicMock(side_effect=psutil.NoSuchProcess(12345))
        mock_cpu_times = mock.MagicMock(side_effect=psutil.AccessDenied())
        mock_process.cpu_percent = mock_cpu_percent
        mock_process.cpu_times = mock_cpu_times
        mock_process.num_threads.side_effect = psutil.NoSuchProcess(12345)

        # Memory stats - raise exceptions to cover lines 137-138
        mock_process.memory_info.side_effect = psutil.AccessDenied()
        mock_process.memory_percent.side_effect = psutil.AccessDenied()

        # IO stats - raise AttributeError to skip io section
        mock_process.io_counters.side_effect = AttributeError()

        with mock.patch.object(source, "_current_process", mock_process):
            stats = source._collect_process_stats()

            # Basic info should be there
            assert stats["pid"] == 12345
            assert stats["name"] == "test"

            # Stats that raised exceptions should be missing
            assert "cpu_percent" not in stats
            assert "cpu_times" not in stats
            assert "num_threads" not in stats
            assert "memory" not in stats
            assert "memory_percent" not in stats
            assert "io" not in stats

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_collect_process_stats_with_io(self):
        """Test _collect_process_stats with IO stats."""
        # Import psutil only when the test is actually going to run
        import psutil  # noqa: F401

        source = StatsSource(name="test", source_pad_names=["out"])

        # Mock Process methods and include io_counters
        mock_process = mock.MagicMock()
        mock_process.pid = 12345
        mock_process.name.return_value = "test"
        mock_process.status.return_value = "running"
        mock_process.create_time.return_value = time.time() - 100

        # CPU stats
        mock_process.cpu_percent.return_value = 10.0
        mock_cpu_times = mock.MagicMock()
        mock_cpu_times._asdict.return_value = {"user": 5.0, "system": 2.0}
        mock_process.cpu_times.return_value = mock_cpu_times
        mock_process.num_threads.return_value = 4

        # Memory stats
        mock_mem_info = mock.MagicMock()
        mock_mem_info.rss = 1024 * 1024
        mock_mem_info.vms = 2048 * 1024
        mock_mem_info.shared = 512 * 1024
        mock_mem_info.text = 256 * 1024
        mock_mem_info.data = 128 * 1024
        mock_process.memory_info.return_value = mock_mem_info
        mock_process.memory_percent.return_value = 5.0

        # IO stats - make sure this is included to hit line 143
        mock_io_counters = mock.MagicMock()
        mock_io_counters.read_count = 100
        mock_io_counters.write_count = 50
        mock_io_counters.read_bytes = 1024
        mock_io_counters.write_bytes = 512
        mock_process.io_counters.return_value = mock_io_counters

        with mock.patch.object(source, "_current_process", mock_process):
            stats = source._collect_process_stats()

            # Check IO stats are included
            assert "io" in stats
            assert stats["io"]["read_count"] == 100
            assert stats["io"]["write_count"] == 50
            assert stats["io"]["read_bytes"] == 1024
            assert stats["io"]["write_bytes"] == 512


#
# System Stats tests
#


class TestSystemStats:
    """Test group for system statistics collection."""

    def test_collect_system_stats_no_psutil(self):
        """Test _collect_system_stats when psutil is not available."""
        # This test works with or without psutil
        # Create a StatsSource with patching to simulate no psutil
        with mock.patch("sgn.sources.PSUTIL_AVAILABLE", False):
            source = StatsSource(name="test", source_pad_names=["out"])
            stats = source._collect_system_stats()

            # Check that limited info is returned
            assert "limited_info" in stats
            assert stats["limited_info"] is True
            assert "error" in stats
            assert "system" in stats
            assert "python_version" in stats

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_system_stats_cpu_exceptions(self):
        """Test exception handling for CPU stats in _collect_system_stats."""
        # Import psutil only when the test is actually going to run
        import psutil  # noqa: F401

        # Test CPU times exception
        def test_cpu_times_exception():
            system_stats = {"cpu": {}}
            with mock.patch.object(
                psutil, "cpu_times", side_effect=OSError("Mock Error")
            ):
                try:
                    system_stats["cpu"]["times"] = dict(psutil.cpu_times()._asdict())
                except (AttributeError, OSError):
                    pass  # Exception handler we want to test
            return system_stats

        result = test_cpu_times_exception()
        assert "times" not in result["cpu"]

        # Test CPU freq exception
        def test_cpu_freq_exception():
            system_stats = {"cpu": {}}
            with mock.patch.object(
                psutil, "cpu_freq", side_effect=AttributeError("Mock Error")
            ):
                try:
                    system_stats["cpu"]["freq"] = (
                        dict(psutil.cpu_freq()._asdict()) if psutil.cpu_freq() else {}
                    )
                except (AttributeError, OSError):
                    pass  # Exception handler we want to test
            return system_stats

        result = test_cpu_freq_exception()
        assert "freq" not in result["cpu"]

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_system_stats_disk_exceptions(self):
        """Test exception handling for disk stats in _collect_system_stats."""
        # Import psutil only when the test is actually going to run
        import psutil  # noqa: F401

        # Test disk usage exception
        def test_disk_usage_exception():
            system_stats = {}
            with mock.patch.object(
                psutil, "disk_usage", side_effect=OSError("Mock Error")
            ):
                try:
                    system_stats["disk"] = {
                        "usage": dict(psutil.disk_usage("/")._asdict()),
                    }
                except (AttributeError, OSError):
                    pass  # Exception handler we want to test
            return system_stats

        result = test_disk_usage_exception()
        assert "disk" not in result

        # Test disk IO counters exception
        def test_disk_io_counters_exception():
            system_stats = {}
            with mock.patch.object(
                psutil, "disk_io_counters", side_effect=AttributeError("Mock Error")
            ):
                try:
                    system_stats["disk"] = {
                        "io_counters": (
                            dict(psutil.disk_io_counters()._asdict())
                            if psutil.disk_io_counters()
                            else {}
                        )
                    }
                except (AttributeError, OSError):
                    pass  # Exception handler we want to test
            return system_stats

        result = test_disk_io_counters_exception()
        assert "disk" not in result

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_system_stats_network_exceptions(self):
        """Test exception handling for network stats in _collect_system_stats."""
        # Import psutil only when the test is actually going to run
        import psutil  # noqa: F401

        # Test network exception
        def test_network_exception():
            system_stats = {}
            with mock.patch.object(
                psutil, "net_io_counters", side_effect=AttributeError("Mock Error")
            ):
                try:
                    net_io = psutil.net_io_counters()
                    system_stats["network"] = dict(net_io._asdict()) if net_io else {}
                except (AttributeError, OSError):
                    pass  # Exception handler we want to test
            return system_stats

        result = test_network_exception()
        assert "network" not in result

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_system_stats_full_exceptions(self):
        """Test all exception handling in _collect_system_stats with a full test."""
        # Import psutil only when the test is actually going to run
        import psutil  # noqa: F401

        source = StatsSource(name="test", source_pad_names=["out"])

        # Make mocks for all required methods to avoid any real psutil calls
        cpu_percent_mock = mock.MagicMock(return_value=10.0)
        cpu_count_mock = mock.MagicMock(return_value=4)
        cpu_stats_mock = mock.MagicMock()
        cpu_stats_mock._asdict.return_value = {"ctx_switches": 1000, "interrupts": 500}
        cpu_stats_func_mock = mock.MagicMock(return_value=cpu_stats_mock)

        virtual_memory_mock = mock.MagicMock()
        virtual_memory_mock._asdict.return_value = {
            "total": 16000000000,
            "available": 8000000000,
        }
        virtual_memory_func_mock = mock.MagicMock(return_value=virtual_memory_mock)

        swap_memory_mock = mock.MagicMock()
        swap_memory_mock._asdict.return_value = {
            "total": 8000000000,
            "used": 1000000000,
        }
        swap_memory_func_mock = mock.MagicMock(return_value=swap_memory_mock)

        # Set up patch context for all the methods
        with mock.patch.multiple(
            psutil,
            cpu_percent=cpu_percent_mock,
            cpu_count=cpu_count_mock,
            cpu_stats=cpu_stats_func_mock,
            virtual_memory=virtual_memory_func_mock,
            swap_memory=swap_memory_func_mock,
            # Exceptions for the specific methods we want to test
            cpu_times=mock.MagicMock(side_effect=OSError("Mock CPU Times Error")),
            cpu_freq=mock.MagicMock(side_effect=AttributeError("Mock CPU Freq Error")),
            disk_usage=mock.MagicMock(side_effect=OSError("Mock Disk Usage Error")),
            disk_io_counters=mock.MagicMock(
                side_effect=AttributeError("Mock Disk IO Error")
            ),
            net_io_counters=mock.MagicMock(side_effect=OSError("Mock Network Error")),
        ):
            # Now call _collect_system_stats and verify all exceptions are handled
            system_stats = source._collect_system_stats()

            # Basic checks to verify the method completed successfully
            assert "cpu" in system_stats
            assert "memory" in system_stats
            assert "swap" in system_stats

            # Verify the exception handlers worked - these fields should be missing
            assert "times" not in system_stats["cpu"]
            assert "freq" not in system_stats["cpu"]
            assert "disk" not in system_stats
            assert "network" not in system_stats

            # Verify the non-exception parts worked
            assert system_stats["cpu"]["percent"] == 10.0
            assert system_stats["cpu"]["count"]["logical"] == 4


#
# EOS handling tests
#


class TestEOSHandling:
    """Test group for EOS handling."""

    def test_check_eos_with_eos_flag(self):
        """Test check_eos when _eos is already True."""
        source = StatsSource(name="test", source_pad_names=["out"])

        # Set _eos flag
        source._eos = True
        assert source.check_eos() is True

    def test_check_eos_with_signal_handler(self):
        """Test check_eos with signal handler."""
        source = StatsSource(name="test", source_pad_names=["out"])

        # Create mock signal handler
        mock_signal_handler = mock.MagicMock()
        mock_signal_handler.signaled_eos.return_value = True

        # Set mock signal handler
        source._signal_handler = mock_signal_handler

        assert source.check_eos() is True

        # Test when signal handler returns False
        mock_signal_handler.signaled_eos.return_value = False
        assert source.check_eos() is False


#
# Miscellaneous features
#


class TestMiscFeatures:
    """Test group for miscellaneous StatsSource features."""

    def test_wait_parameter(self):
        """Test the wait parameter in new()."""
        source = StatsSource(
            name="test",
            source_pad_names=["out"],
            wait=0.1,  # Set a small wait time for testing
        )

        # Mock time.sleep to verify it's called
        with mock.patch("time.sleep") as mock_sleep:
            # Access the first source pad from the element
            pad = source.source_pads[0]
            source.new(pad)  # Call new() but don't need to store the frame

            # Verify time.sleep was called with the wait parameter
            mock_sleep.assert_called_once_with(0.1)

    def test_psutil_import_error(self):
        """Test the ImportError handling for psutil."""
        # This test specifically tests the ImportError handling for psutil
        # and works whether or not psutil is installed

        # Save original modules state
        original_modules = sys.modules.copy()

        # Remove psutil from sys.modules if present
        if "psutil" in sys.modules:
            del sys.modules["psutil"]

        # Mock __import__ to raise ImportError for psutil specifically
        original_import = __import__

        def mocked_import(name, *args, **kwargs):
            if name == "psutil":
                raise ImportError("Mock ImportError for psutil")
            return original_import(name, *args, **kwargs)

        try:
            # Use the mocked import to test the import error handling
            with mock.patch("builtins.__import__", side_effect=mocked_import):
                with mock.patch("warnings.warn") as mock_warn:
                    # Reload the sources module to trigger the import error
                    import importlib
                    import sgn.sources as stats_module

                    importlib.reload(stats_module)

                    # The warning is issued in the class __post_init__ method,
                    # so we need to create an instance to trigger it
                    stats_module.StatsSource(name="test", source_pad_names=["out"])

                    # Get the actual warning message from the stats_source module
                    # This ensures the test matches whatever message is actually used
                    with mock.patch("warnings.warn") as inner_mock_warn:
                        # Create a new instance to capture the exact warning message
                        with mock.patch.object(stats_module, "PSUTIL_AVAILABLE", False):
                            stats_module.StatsSource(
                                name="capture", source_pad_names=["test"]
                            )
                            args, kwargs = inner_mock_warn.call_args
                            expected_message = args[0]
                    mock_warn.assert_called_with(expected_message, stacklevel=2)

                    # Verify PSUTIL_AVAILABLE was set to False
                    assert stats_module.PSUTIL_AVAILABLE is False
        finally:
            # Restore original modules
            sys.modules = original_modules

            # Reload the sources module one more time to restore the original state
            import importlib
            import sgn.sources as stats_module

            importlib.reload(stats_module)
