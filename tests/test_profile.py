"""Unit tests for the apps module."""


def test_mem_prof(monkeypatch):
    monkeypatch.setenv("SGNLOGLEVEL", "pipeline:MEMPROF")
    import sgn
    import sgn.apps
    import sgn.profile
    import importlib

    importlib.reload(sgn)
    importlib.reload(sgn.apps)
    importlib.reload(sgn.profile)
    from sgn import NullSink, NullSource
    from sgn.apps import Pipeline

    p = Pipeline()
    e1 = NullSource(name="src1", source_pad_names=("H1",), num_frames=2)
    e2 = NullSink(sink_pad_names=("H1",))
    p.insert(e1, e2, link_map={e2.snks["H1"]: e1.srcs["H1"]})
    p.run()
    monkeypatch.delenv("SGNLOGLEVEL")
