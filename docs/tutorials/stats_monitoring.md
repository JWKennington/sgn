# System Statistics Monitoring

This tutorial demonstrates how to use the `StatsSource` class to monitor system statistics in real-time within your SGN pipeline.

## Overview

The `StatsSource` class is a specialized source element that periodically collects system and process statistics using the `psutil` library. It can be used to monitor the resource usage of your application and the overall system performance.

## Prerequisites

This tutorial requires the `psutil` library. You can install it with pip:

```bash
pip install psutil
```

## Basic Usage

Here's a simple example that collects system statistics every 2 seconds and prints them to the console:

```python
#!/usr/bin/env python3
"""Example of using StatsSource to monitor system resources.

This example creates a pipeline with a StatsSource that collects
system statistics and outputs them to a print sink.
"""

import time
from pprint import pprint

from sgn.base import Frame, SinkElement, SinkPad
from sgn.apps import Pipeline
from sgn.sources import SignalEOS, StatsSource


class PrintSink(SinkElement):
    """Simple sink that prints received frames."""

    def __init__(self, name=None, sink_pad_names=None):
        super().__init__(name=name, sink_pad_names=sink_pad_names)

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        """Print the data in the frame received on the pad.

        Args:
            pad: SinkPad, the pad that the frame is pulled into
            frame: Frame, the frame that is pulled into the sink
        """
        print(f"\n--- Stats from {pad.name} at {time.ctime()} ---")
        if frame.data:
            pprint(frame.data)
        else:
            print("No data in frame")

        if frame.EOS:
            print(f"End of stream detected on {pad.name}")
            self.mark_eos(pad)


def main():
    """Run a sample pipeline with StatsSource."""
    # Create a pipeline
    pipeline = Pipeline()

    # Create a stats source element with 2-second interval
    stats_source = StatsSource(
        name="system_monitor",
        source_pad_names=["stats"],
        interval=2.0,  # Collect stats every 2 seconds
        include_process_stats=True,
        include_system_stats=True,
        # Wait for consistent timing between frames
        wait=2.0,  # Sleep for 2 seconds between frames
    )

    # Create a sink to print the stats
    stats_sink = PrintSink(name="stats_printer", sink_pad_names=["stats_in"])

    # Add elements to the pipeline
    pipeline.insert(stats_source)
    pipeline.insert(stats_sink)

    # Link the source to the sink
    # The format for pad names is: "element_name:pad_type:pad_name"
    pipeline.link({"stats_printer:snk:stats_in": "system_monitor:src:stats"})

    print("Starting stats monitoring pipeline...")
    print("Press Ctrl+C to stop")

    # Run the pipeline with signal handling
    with SignalEOS():  # Context manager for signal handling
        pipeline.run()

    print("Pipeline finished")


if __name__ == "__main__":
    # Note: This example requires the psutil package
    # Install with: pip install psutil
    main()
```

## Customizing Stats Collection

The `StatsSource` class provides several parameters to customize the statistics collection:

### Parameters

- `interval`: Time in seconds between stats collection. If `None`, stats are collected every time `new()` is called.
- `include_process_stats`: Whether to include statistics about the current process.
- `include_system_stats`: Whether to include system-wide statistics. 
- `eos_on_signal`: Whether to end the stream on receiving SIGINT/SIGTERM signals.
- `wait`: Time in seconds to wait between frames. This can help establish a consistent timing pattern.

## Output Format

The output from StatsSource is a Frame containing a dictionary with system and process statistics. Here's a sample of the data structure:

```python
{
    "process": {
        "pid": 12345,
        "name": "python",
        "status": "running",
        "created": 1620123456.789,
        "running_time": 123.45,
        "cpu_percent": 2.5,
        "cpu_times": {
            "user": 1.23,
            "system": 0.45
        },
        "num_threads": 1,
        "memory": {
            "rss": 52428800,
            "vms": 104857600,
            "shared": 8388608,
            "text": 4194304,
            "data": 16777216
        },
        "memory_percent": 0.5,
        "io": {
            "read_count": 100,
            "write_count": 50,
            "read_bytes": 1024,
            "write_bytes": 512
        }
    },
    "system": {
        "cpu": {
            "percent": 15.0,
            "count": {
                "physical": 4,
                "logical": 8
            },
            "stats": {
                "ctx_switches": 10000,
                "interrupts": 5000,
                "soft_interrupts": 2500,
                "syscalls": 20000
            },
            "times": {
                "user": 10000.0,
                "system": 5000.0,
                "idle": 50000.0
            },
            "freq": {
                "current": 2500.0,
                "min": 1200.0,
                "max": 3500.0
            }
        },
        "memory": {
            "total": 17179869184,
            "available": 8589934592,
            "percent": 50.0,
            "used": 8589934592,
            "free": 8589934592
        },
        "swap": {
            "total": 8589934592,
            "used": 1073741824,
            "free": 7516192768,
            "percent": 12.5
        },
        "disk": {
            "usage": {
                "total": 1099511627776,
                "used": 549755813888,
                "free": 549755813888,
                "percent": 50.0
            },
            "io_counters": {
                "read_count": 1000,
                "write_count": 500,
                "read_bytes": 10485760,
                "write_bytes": 5242880
            }
        },
        "network": {
            "bytes_sent": 1048576,
            "bytes_recv": 5242880,
            "packets_sent": 1000,
            "packets_recv": 5000,
            "errin": 0,
            "errout": 0,
            "dropin": 0,
            "dropout": 0
        }
    },
    "timestamp": 1620123456.789
}
```

The exact fields available will depend on your system and the capabilities of the `psutil` library on your platform.

## Fallback Behavior with psutil Missing

If `psutil` is not available, `StatsSource` will still function but with limited capabilities:

- A warning will be displayed when creating the `StatsSource`
- Process statistics will only include the process ID and a message indicating limited functionality
- System statistics will include basic information like the platform and Python version

This allows your pipeline to run even without `psutil`, though with reduced monitoring capabilities.

## Signal Handling

The example uses the `SignalEOS` context manager to handle SIGINT and SIGTERM signals. This allows the pipeline to terminate gracefully when you press Ctrl+C.

## Conclusion

The `StatsSource` element provides a way to monitor system and process statistics within your SGN pipeline. It's useful for:

- Debugging application performance issues
- Monitoring resource usage in long-running applications
- Creating system monitoring tools
- Gathering performance metrics for analysis

By integrating system monitoring directly into your SGN pipeline, you can react to resource constraints or performance issues in real-time.