# Memory Profiling in SGN

This tutorial explains how to use SGN's memory profiling capabilities to monitor and optimize your pipelines.

## Overview

SGN provides built-in memory profiling tools that can help you:

- Track memory usage across your pipeline
- Identify memory leaks
- Find components that consume excessive memory
- Optimize memory-intensive operations

The memory profiling functionality is implemented in the `profile.py` module and integrates with SGN's logging system.

## Enabling Memory Profiling

Memory profiling in SGN is disabled by default. To enable it, you need to set the `SGNLOGLEVEL` environment variable to include the `MEMPROF` level for the components you want to profile.

### Environment Variable Setup

You can enable memory profiling in two ways:

#### 1. Profile the entire pipeline:

```bash
export SGNLOGLEVEL="pipeline:MEMPROF"
```

#### 2. Profile specific components:

```bash
export SGNLOGLEVEL="pipeline:MEMPROF source:INFO sink:INFO"
```

This enables memory profiling for the pipeline while keeping normal logging for source and sink components.

## Basic Memory Profiling Example

Here's a simple example that demonstrates how to enable memory profiling in an SGN pipeline:

```{.python notest}
#!/usr/bin/env python3

import os
from sgn.apps import Pipeline
from sgn.base import SourceElement, SinkElement, Frame

# Set environment variable to enable memory profiling
os.environ["SGNLOGLEVEL"] = "pipeline:MEMPROF"

class SimpleSource(SourceElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.counter = 0
        self.max_frames = 10

    def new(self, pad):
        # Generate a new frame with incrementing counter
        self.counter += 1
        if self.counter > self.max_frames:
            return Frame(data=f"Frame {self.counter}", EOS=True)
        return Frame(data=f"Frame {self.counter}")

class SimpleSink(SinkElement):
    def pull(self, pad, frame):
        # Process incoming frame
        print(f"Received: {frame.data}")
        if frame.EOS:
            self.mark_eos(pad)

# Create pipeline elements
source = SimpleSource(source_pad_names=("out",))
sink = SimpleSink(sink_pad_names=("in",))

# Create and connect pipeline
pipeline = Pipeline()
pipeline.insert(source, sink, link_map={sink.snks["in"]: source.srcs["out"]})

# Run the pipeline with memory profiling enabled
pipeline.run()
```

When you run this example, you'll see memory profiling information in the output showing memory allocation statistics.

## Understanding the Memory Profiling Output

The memory profiling output consists of several sections:

### 1. Cumulative Memory Usage

This section shows the top memory-consuming lines in your code:

```
[MEMPROF] -------------------------------------------------
[MEMPROF] | Top 10 lines of memory usage: cumulative
[MEMPROF] |
[MEMPROF] | #1: /path/to/file.py:123: 8.7 KiB
[MEMPROF] |     some_code_line_here
[MEMPROF] | #2: /path/to/file.py:456: 2.7 KiB
[MEMPROF] |     another_code_line
...
[MEMPROF] | Total allocated size: 26.1 KiB
```

### 2. Differential Memory Usage

This section shows the changes in memory usage between profiling snapshots:

```
[MEMPROF] | Top 10 lines of memory usage: diff from previous
[MEMPROF] |
[MEMPROF] | #1: /path/to/file.py:789: 0.8 KiB
[MEMPROF] |     some_allocation
...
[MEMPROF] | Total allocated size: 31.7 KiB
[MEMPROF] | Change from start 0.0 KiB
```

The "Change from start" value helps you track memory growth over time, which can indicate memory leaks.


## Conclusion

Memory profiling is a powerful tool for understanding and optimizing the memory usage of your SGN pipelines. By enabling the built-in memory profiling capabilities, you can:

- Identify memory bottlenecks
- Track memory growth over time
- Find and fix memory leaks
- Optimize memory-intensive operations

Remember to disable memory profiling in production environments as it adds some overhead to your application.
