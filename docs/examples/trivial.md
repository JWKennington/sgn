# Example: Trivial Pipeline

This example shows a trivial pipeline, containing only a source and a sink.
The source is a `NullSource`, which produces empty `Frame` objects. The sink is a
`NullSink`, which consumes the `Frame` objects produced by the source, but does
nothing with them.

## Code

```python
from sgn import Pipeline, NullSource, NullSink

# Create pipeline in one go
p = Pipeline()
p.insert(NullSource(name='src1',
                    source_pad_names=["H1"],
                    num_frames=1),
         NullSink(name='snk1',
                  sink_pad_names=["H1"]),
         link_map={"snk1:snk:H1": "src1:src:H1"})
p.run()
```
