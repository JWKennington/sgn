"""Test transforms module
"""
from sgn.base import Frame
from sgn.transforms import FakeTransform


class TestFakeTransform:
    """Test group for FakeSink class"""

    def test_init(self):
        """Test the FakeSink class constructor"""
        trn = FakeTransform(name='t1', source_pad_names=('I1',), sink_pad_names=('H1',))
        assert isinstance(trn, FakeTransform)
        assert [p.name for p in trn.source_pads] == ['t1:src:I1']
        assert [p.name for p in trn.sink_pads] == ['t1:sink:H1']

    def test_pull(self):
        """Test pull"""
        trn = FakeTransform(name='t1', source_pad_names=('I1',), sink_pad_names=('H1',))
        assert trn.inputs == {}

        trn.pull(trn.source_pads[0], Frame(metadata={'name': 'F1'}))
        assert trn.inputs[trn.source_pads[0]] == Frame(metadata={'name': 'F1'})

    def test_transform(self):
        """Test transform"""
        trn = FakeTransform(name='t1', source_pad_names=('I1',), sink_pad_names=('H1',))
        trn.inputs[trn.source_pads[0]] = Frame(metadata={'name': 'F1'})
        frame = trn.transform(trn.source_pads[0])
        assert frame.metadata['name'] == 'F1 -> t1:src:I1'
        assert not frame.EOS
        assert trn.inputs == {trn.source_pads[0]: Frame(metadata={'name': 'F1'})}
