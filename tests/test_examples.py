"""Tests for end-to-end examples used in the docs.

Each test here should be fully self contained, including imports, to ensure that the
examples in the docs are correct and up-to-date.
"""


class TestExamples:
    """Test the examples in the docs."""

    def test_example_trivial(self):
        """Test a zeroth example."""
        from sgn import NullSink, NullSource, Pipeline

        # Create pipeline in one go
        p = Pipeline()
        p.insert(
            NullSource(name="src1", source_pad_names=["H1"]),
            NullSink(name="snk1", sink_pad_names=["H1"]),
            link_map={"snk1:sink:H1": "src1:src:H1"},
        )
        p.run()

    def test_example_simple(self):
        """Test the simple example."""
        import functools

        from sgn import CallableTransform, CollectSink, IterSource, Pipeline

        # Define a function to use in the pipeline
        def scale(frame, factor: float):
            return None if frame.data is None else frame.data * factor

        # Create source element
        src = IterSource(
            name="src1",
            source_pad_names=["H1"],
            iters={"src1:src:H1": [1, 2, 3]},
        )

        # Create a transform element using an arbitrary function
        trn1 = CallableTransform.from_callable(
            name="t1",
            sink_pad_names=["H1"],
            callable=functools.partial(scale, factor=10),
            output_pad_name="H1",
        )

        # Create the sink so we can access the data after running
        snk = CollectSink(
            name="snk1",
            sink_pad_names=("H1",),
        )

        # Create the Pipeline
        p = Pipeline()

        # Insert elements into pipeline and link them explicitly
        p.insert(
            src,
            trn1,
            snk,
            link_map={
                "t1:sink:H1": "src1:src:H1",
                "snk1:sink:H1": "t1:src:H1",
            },
        )

        # Run the pipeline
        p.run()

        # Check the result of the sink queue to see outputs
        assert list(snk.collects["snk1:sink:H1"]) == [10, 20, 30]

    def test_example_scalars(self):
        """Test the first example."""
        from sgn import CallableTransform, DequeSink, DequeSource, Pipeline

        # Define a function to use in the pipeline
        def add_ten(frame):
            return None if frame.data is None else frame.data + 10

        # Create source element
        src = DequeSource(
            name="src1",
            source_pad_names=["H1"],
            iters={"src1:src:H1": [1, 2, 3]},
        )

        # Create a transform element using an arbitrary function
        trn1 = CallableTransform.from_callable(
            name="t1",
            sink_pad_names=["H1"],
            callable=add_ten,
            output_pad_name="H1",
        )

        # Create the sink so we can access the data after running
        snk = DequeSink(
            name="snk1",
            sink_pad_names=("H1",),
        )

        # Create the Pipeline
        p = Pipeline()

        # Insert elements into pipeline and link them explicitly
        p.insert(
            src,
            trn1,
            snk,
            link_map={
                "t1:sink:H1": "src1:src:H1",
                "snk1:sink:H1": "t1:src:H1",
            },
        )

        # Run the pipeline
        p.run()

        # Check the result of the sink queue to see outputs
        assert list(snk.deques["snk1:sink:H1"]) == [13, 12, 11]

    def test_example_json(self):
        """Test example with json payloads that have the following schema:

        {
            "time": datetime.datetime,
            "buffer": numpy.ndarray,
            "trusted": bool,
        }
        """
        import datetime

        import numpy

        from sgn import CallableTransform, CollectSink, IterFrame, IterSource, Pipeline

        # Define the payloads
        payloads = [
            # Payload 1, one trusted one not
            [
                {
                    "time": datetime.datetime.strptime(
                        "2021-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                    ),
                    "buffer": numpy.array([1.0, 2.0, 3.0]),
                    "trusted": True,
                },
                {
                    "time": datetime.datetime.strptime(
                        "2021-01-01T00:00:01", "%Y-%m-%dT%H:%M:%S"
                    ),
                    "buffer": numpy.array([1.0, numpy.nan, 3.0]),
                    "trusted": False,
                },
            ],
            # Payload 2, both trusted
            [
                {
                    "time": datetime.datetime.strptime(
                        "2021-01-01T00:00:02", "%Y-%m-%dT%H:%M:%S"
                    ),
                    "buffer": numpy.array([4.0, 5.0, 6.0]),
                    "trusted": True,
                },
                {
                    "time": datetime.datetime.strptime(
                        "2021-01-01T00:00:03", "%Y-%m-%dT%H:%M:%S"
                    ),
                    "buffer": numpy.array([7.0, 8.0, 9.0]),
                    "trusted": True,
                },
            ],
        ]

        # Define a function to use in the pipeline
        def demean_if_trusted(frame: IterFrame):
            if frame.data is None:
                return None

            results = []
            for packet in frame.data:
                new_packet = packet.copy()
                if new_packet["trusted"]:
                    new_packet["buffer"] -= numpy.mean(new_packet["buffer"])
                results.append(new_packet)
            return results

        # Create source element
        src = IterSource(
            name="src1",
            source_pad_names=["H1"],
            iters={"src1:src:H1": payloads},
            frame_factory=IterFrame,
        )

        # Create a transform element using an arbitrary function
        trn1 = CallableTransform.from_callable(
            name="t1",
            sink_pad_names=["H1"],
            callable=demean_if_trusted,
            output_pad_name="H1",
        )

        # Create the sink so we can access the data after running
        snk = CollectSink(
            name="snk1",
            sink_pad_names=("H1",),
        )

        # Create the Pipeline
        p = Pipeline()

        # Insert elements into pipeline and link them explicitly
        p.insert(
            src,
            trn1,
            snk,
            link_map={
                "t1:sink:H1": "src1:src:H1",
                "snk1:sink:H1": "t1:src:H1",
            },
        )

        # Run the pipeline
        p.run()

        # Check the result of the sink queue to see outputs
        # We check each packet individually to avoid numpy array comparison issues
        result = list(snk.collects["snk1:sink:H1"])
        expected = [
            [
                {
                    "time": datetime.datetime(2021, 1, 1, 0, 0, 0),
                    "buffer": numpy.array([-1.0, 0.0, 1.0]),
                    "trusted": True,
                },
                {
                    "time": datetime.datetime(2021, 1, 1, 0, 0, 1),
                    "buffer": numpy.array([1.0, numpy.nan, 3.0]),
                    "trusted": False,
                },
            ],
            [
                {
                    "time": datetime.datetime(2021, 1, 1, 0, 0, 2),
                    "buffer": numpy.array([-1.0, 0.0, 1.0]),
                    "trusted": True,
                },
                {
                    "time": datetime.datetime(2021, 1, 1, 0, 0, 3),
                    "buffer": numpy.array([-1.0, 0.0, 1.0]),
                    "trusted": True,
                },
            ],
        ]
        for res, exp in zip(result, expected):
            for r_pack, e_pack in zip(res, exp):
                for key in r_pack:
                    if isinstance(r_pack[key], numpy.ndarray):
                        assert numpy.allclose(r_pack[key], e_pack[key], equal_nan=True)
                    else:
                        assert r_pack[key] == e_pack[key]
