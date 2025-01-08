"""Test transforms module."""

import pytest

from sgn.base import Frame
from sgn.transforms import CallableTransform, InputPull


class TestInputPull:
    """Test group for FakeSink class."""

    def test_init(self):
        """Test the InputPull class constructor."""
        trn = InputPull(
            name="t1", source_pad_names=("I1", "I2"), sink_pad_names=("O1", "O2")
        )
        assert isinstance(trn, InputPull)

    def test_pull(self):
        """Test pull method."""
        trn = InputPull(
            name="t1", source_pad_names=("I1", "I2"), sink_pad_names=("O1", "O2")
        )
        trn.pull(trn.source_pads[0], Frame(data=2))
        assert trn.inputs["t1:src:I1"].data == 2
        trn.pull(trn.source_pads[1], Frame(data=3))
        assert trn.inputs["t1:src:I2"].data == 3


class TestCallableTransform:
    """Test group for CallableTransform class."""

    def test_init(self):
        """Test the CallableTransform class constructor."""
        identity = lambda x: x
        trn = CallableTransform(
            name="t1",
            callmap={
                "O1": identity,
                "O2": identity,
            },
            depmap={
                "O1": ("I1",),
                "O2": ("I2",),
            },
        )
        assert isinstance(trn, CallableTransform)
        assert [p.name for p in trn.sink_pads] == ["t1:snk:I1", "t1:snk:I2"]
        assert [p.name for p in trn.source_pads] == ["t1:src:O1", "t1:src:O2"]
        assert trn.callmap == {
            "t1:src:O1": identity,
            "t1:src:O2": identity,
        }
        assert trn.depmap == {
            "t1:src:O1": ("t1:snk:I1",),
            "t1:src:O2": ("t1:snk:I2",),
        }

    def test_init_fully_formatted_keys(self):
        """Test the CallableTransform class constructor."""
        identity = lambda x: x
        trn = CallableTransform(
            name="t1",
            callmap={
                "t1:src:O1": identity,
                "t1:src:O2": identity,
            },
            depmap={
                "t1:src:O1": ("t1:snk:I1",),
                "t1:src:O2": ("t1:snk:I2",),
            },
        )
        assert isinstance(trn, CallableTransform)
        assert [p.name for p in trn.sink_pads] == ["t1:snk:I1", "t1:snk:I2"]
        assert [p.name for p in trn.source_pads] == ["t1:src:O1", "t1:src:O2"]
        assert trn.callmap == {
            "t1:src:O1": identity,
            "t1:src:O2": identity,
        }
        assert trn.depmap == {
            "t1:src:O1": ("t1:snk:I1",),
            "t1:src:O2": ("t1:snk:I2",),
        }

    def test_init_err_no_depmap(self):
        """Test the CallableTransform class constructor."""
        identity = lambda x: x
        with pytest.raises(ValueError):
            CallableTransform(
                name="t1",
                sink_pad_names=("I1", "I2"),
                callmap={
                    "O1": identity,
                    "O2": identity,
                },
            )

    def test_init_err_src_info(self):
        """Test the CallableTransform class constructor error case."""
        with pytest.raises(ValueError):
            CallableTransform(
                name="t1",
                source_pad_names=("I1", "I2"),
                sink_pad_names=("O1", "O2"),
                callmap={("I1",): lambda x: x, ("I2",): lambda x: x},
                depmap={("I1",): "O1", ("I2",): "O2"},
            )

        with pytest.raises(ValueError):
            CallableTransform(
                name="t1",
                source_pads=("I1", "I2"),
                sink_pad_names=("O1", "O2"),
                callmap={("I1",): lambda x: x, ("I2",): lambda x: x},
                depmap={("I1",): "O1", ("I2",): "O2"},
            )

    def test_init_err_no_callmap(self):
        """Test the CallableTransform class constructor error case."""
        with pytest.raises(ValueError):
            CallableTransform(
                name="t1",
                sink_pad_names=("I1", "I2"),
                depmap={("I1",): "O1", ("I2",): "O2"},
            )

    def test_init_err_mismatched_keys(self):
        """Test the CallableTransform class constructor error case."""
        with pytest.raises(ValueError):
            CallableTransform(
                name="t1",
                callmap={
                    "O1": lambda x: x,
                    "O2": lambda x: x,
                },
                depmap={
                    "O1": ("I1",),
                    "O3": ("I2",),
                },
            )

    def test_transform(self):
        """Test transform."""
        trn = CallableTransform(
            name="t1",
            sink_pad_names=("I1", "I2"),
            callmap={
                "O1": lambda f1, f2: f1.data + f2.data,
                "O2": lambda f: f.data * 10,
            },
            depmap={
                "O1": ("I1", "I2"),
                "O2": ("I2",),
            },
        )

        # Setup data on sink pads (usually handled by pull method)
        trn.inputs[trn.sink_pads[0].name] = Frame(data=2)
        trn.inputs[trn.sink_pads[1].name] = Frame(data=3)
        f0 = trn.new(trn.source_pads[0])
        f1 = trn.new(trn.source_pads[1])
        assert not f0.EOS
        assert f0.data == 5
        assert f1.data == 30

    def test_from_combinations(self):
        """Test from_combinations."""
        func = lambda f1, f2: f1.data + f2.data
        func2 = lambda f: f.data
        trn = CallableTransform.from_combinations(
            name="t1",
            combos=[
                (("I1", "I2"), func, "O1"),
                (("I2",), func2, "O2"),
            ],
        )
        assert isinstance(trn, CallableTransform)
        assert trn.callmap == {
            "t1:src:O1": func,
            "t1:src:O2": func2,
        }
        assert trn.depmap == {
            "t1:src:O1": ("t1:snk:I1", "t1:snk:I2"),
            "t1:src:O2": ("t1:snk:I2",),
        }

    def test_from_combinations_multiple(self):
        """Test from_combinations."""
        func = lambda f1, f2: f1.data + f2.data
        trn = CallableTransform.from_combinations(
            name="t1",
            combos=[
                (("I1", "I2"), func, "O1"),
                (("I1", "I2"), func, "O2"),
            ],
        )
        assert isinstance(trn, CallableTransform)
        assert [p.name for p in trn.source_pads] == ["t1:src:O1", "t1:src:O2"]
        assert trn.callmap == {
            "t1:src:O1": func,
            "t1:src:O2": func,
        }
        assert trn.depmap == {
            "t1:src:O1": ("t1:snk:I1", "t1:snk:I2"),
            "t1:src:O2": ("t1:snk:I1", "t1:snk:I2"),
        }

    def test_from_callable(self):
        """Test from_callable."""
        func = lambda f1, f2: f1.data + f2.data
        trn = CallableTransform.from_callable(
            name="t1", sink_pad_names=("I1", "I2"), callable=func, output_pad_name="O1"
        )
        assert isinstance(trn, CallableTransform)
        assert trn.callmap == {
            "t1:src:O1": func,
        }
        assert trn.depmap == {
            "t1:src:O1": ("t1:snk:I1", "t1:snk:I2"),
        }
