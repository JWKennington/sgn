#!/usr/bin/env python3

from sgn.apps import Pipeline
from sgn.sinks import FakeSink
from sgn.sources import FakeSrc
from sgn.transforms import FakeTransform


def test_graph(capsys):

    pipeline = Pipeline()

    #
    #          ------                                ------
    #         | src1 |                              | src2 |
    #          ------                                ------
    #         /       \---------------            V1 |    | K1
    #     H1 /         \ L1           \ L1           |    |
    #   ----------    -----------    ----------    ------------
    #  | trans1   |  | trans2    |  | trans3   |  | trans4     |
    #   ----------    -----------    ----------    ------------
    #          \      /                     \     /      /
    #       H1  \    / L1                 L1 \   / V1   / K1
    #           ------                        -----------
    #          | snk1 |                      | snk2      |
    #           ------                        -----------
    #

    pipeline.insert(
        FakeSrc(name="src1", source_pad_names=("H1", "L1"), num_buffers=2)
    ).insert(
        FakeTransform(
            name="trans1",
            source_pad_names=("H1",),
            sink_pad_names=("H1",),
        ),
        link_map={"trans1:sink:H1": "src1:src:H1"},
    ).insert(
        FakeSink(
            name="snk1",
            sink_pad_names=("H1", "L1"),
        ),
        link_map={"snk1:sink:H1": "trans1:src:H1"},
    )

    pipeline.insert(
        FakeTransform(
            name="trans2",
            source_pad_names=("L1",),
            sink_pad_names=("L1",),
        ),
        link_map={"trans2:sink:L1": "src1:src:L1"},
    ).link(link_map={"snk1:sink:L1": "trans2:src:L1"})

    pipeline.insert(
        FakeTransform(
            name="trans3",
            source_pad_names=("L1",),
            sink_pad_names=("L1",),
        ),
        link_map={"trans3:sink:L1": "src1:src:L1"},
    ).insert(
        FakeSink(
            name="snk2",
            sink_pad_names=("L1", "V1", "K1"),
        ),
        link_map={"snk2:sink:L1": "trans3:src:L1"},
    )

    pipeline.insert(FakeSrc(name="src2", source_pad_names=("V1", "K1"), num_buffers=2))
    pipeline.insert(
        FakeTransform(
            name="trans4",
            source_pad_names=("V1", "K1"),
            sink_pad_names=("V1", "K1"),
        )
    )
    pipeline.link(
        {
            "trans4:sink:V1": "src2:src:V1",
            "trans4:sink:K1": "src2:src:K1",
            "snk2:sink:V1": "trans4:src:V1",
            "snk2:sink:K1": "trans4:src:K1",
        }
    )

    pipeline.run()
    if capsys is not None:
        captured = capsys.readouterr()
        assert (
            captured.out.strip()
            == """
buffer flow:  src1:src:H1[0] -> trans1:src:H1 -> snk1:sink:H1
buffer flow:  src1:src:L1[0] -> trans2:src:L1 -> snk1:sink:L1
buffer flow:  src1:src:L1[0] -> trans3:src:L1 -> snk2:sink:L1
buffer flow:  src2:src:V1[0]+src2:src:K1[0] -> trans4:src:V1 -> snk2:sink:V1
buffer flow:  src2:src:V1[0]+src2:src:K1[0] -> trans4:src:K1 -> snk2:sink:K1
buffer flow:  src1:src:H1[1] -> trans1:src:H1 -> snk1:sink:H1
buffer flow:  src1:src:L1[1] -> trans2:src:L1 -> snk1:sink:L1
buffer flow:  src1:src:L1[1] -> trans3:src:L1 -> snk2:sink:L1
buffer flow:  src2:src:V1[1]+src2:src:K1[1] -> trans4:src:V1 -> snk2:sink:V1
buffer flow:  src2:src:V1[1]+src2:src:K1[1] -> trans4:src:K1 -> snk2:sink:K1
""".strip()
        )


if __name__ == "__main__":
    test_graph(None)
