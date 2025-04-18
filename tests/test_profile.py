"""Unit tests for the apps module."""

import os
import importlib

import sgn
import sgn.apps


from sgn import NullSink, NullSource
from sgn.apps import Pipeline

os.environ["SGNLOGLEVEL"] = "pipeline:MEMPROF"
importlib.reload(sgn)
importlib.reload(sgn.apps)


def test_mem_prof():
    p = Pipeline()
    e1 = NullSource(name="src1", source_pad_names=("H1",), num_frames=2)
    e2 = NullSink(sink_pad_names=("H1",))
    p.insert(e1, e2, link_map={e2.snks["H1"]: e1.srcs["H1"]})
    p.run()


if __name__ == "__main__":
    test_mem_prof()
