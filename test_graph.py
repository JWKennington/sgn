#!/usr/bin/env python3
from sgn.apps import Pipeline

def test_graph(capsys): 

    pipeline = Pipeline()
    
    #
    #          ------                                ------
    #         | src1 |                              | src2 |
    #          ------                                ------
    #         /       \---------------            V1 |    | K1
    #     H1 /         \ L1           \ L1           |    |
    #   ----------    -----------    ----------    ------------
    #  | H1trans1 |  | L1trans1  |  | L1trans2 |  | V1K1trans1 |
    #   ----------    -----------    ----------    ------------
    #          \      /                     \     /      /
    #       H1  \    / L1                 L1 \   / V1   / K1
    #           ------                        -----------
    #          | snk1 |                      | snk2      |
    #           ------                        -----------
    #
    
    
    pipeline.FakeSrc(
               name = "src1",
               channels = ("H1","L1"),
               num_buffers = 2
             ).FakeTransform(
               name = "H1trans1", src_pad_name = "src1:H1:src", in_channels = ("H1",), out_channels = ("H1",)
             ).FakeSink(
               name = "snk1",
               channels = ("H1","L1"),
               sink_pad_name = "snk1:H1:sink"
             )
    
    pipeline.FakeTransform(
               name = "L1trans1",
               src_pad_name = "src1:L1:src", in_channels = ("L1",), out_channels = ("L1",)
             ).link(
               src_pad_name = "L1trans1:L1:src",
               sink_pad_name = "snk1:L1:sink"
             )
    
    pipeline.FakeTransform(
               name = "L1trans2",
               src_pad_name = "src1:L1:src",
               head = "src1", in_channels = ("L1",), out_channels = ("L1",)
             ).FakeSink(
               name = "snk2",
               channels = ("L1","V1","K1"),
               sink_pad_name = "snk2:L1:sink"
             )
    
    pipeline.FakeSrc(
               name = "src2",
               channels = ("V1","K1"),
               num_buffers = 2
             ).FakeTransform(
               name = "V1K1trans1", src_pad_name = "src2:V1:src", in_channels = ("V1","K1"), out_channels = ("V1","K1"), sink_pad_name = "V1K1trans1:V1:sink"
             ).link(
               src_pad_name = "src2:K1:src",
               sink_pad_name = "V1K1trans1:K1:sink"
             ).link(
               src_pad_name = "V1K1trans1:K1:src",
               sink_pad_name = "snk2:K1:sink"
             ).link(
               src_pad_name = "V1K1trans1:V1:src",
               sink_pad_name = "snk2:V1:sink"
             )
    
    pipeline.run()
    if capsys is not None:
        captured = capsys.readouterr()
        assert captured.out.strip() == """
buffer flow:  'src1:H1:src' -> 'H1trans1:H1:src' -> 'snk1:H1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans1:L1:src' -> 'snk1:L1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans2:L1:src' -> 'snk2:L1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:V1:src' -> 'snk2:V1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:K1:src' -> 'snk2:K1:sink'
buffer flow:  'src1:H1:src' -> 'H1trans1:H1:src' -> 'snk1:H1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans1:L1:src' -> 'snk1:L1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans2:L1:src' -> 'snk2:L1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:V1:src' -> 'snk2:V1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:K1:src' -> 'snk2:K1:sink'
buffer flow:  'src1:H1:src' -> 'H1trans1:H1:src' -> 'snk1:H1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans1:L1:src' -> 'snk1:L1:sink'
buffer flow:  'src1:L1:src' -> 'L1trans2:L1:src' -> 'snk2:L1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:V1:src' -> 'snk2:V1:sink'
buffer flow:  'src2:V1:src'+'src2:K1:src' -> 'V1K1trans1:K1:src' -> 'snk2:K1:sink'
""".strip()

if __name__ == "__main__":
    test_graph(None)
