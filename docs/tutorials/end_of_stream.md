# End of Stream (EOS)

A program executes normally when all of its source elements mark frames as end
of stream (EOS) and all the sink elements receive EOS frames.  Here is a program
that sends EOS after 10 frames.

```
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class MySourceClass(SourceElement):
    def __post_init__(self):
        super().__post_init__()
        self.cnt = 0
    def new(self, pad):
        self.cnt += 1
        return Frame(data="hello", EOS=self.cnt > 10)

class MySinkClass(SinkElement):
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        print (frame.data)

source = MySourceClass(source_pad_names = ("a",))
sink = MySinkClass(sink_pad_names = ("x",))

pipeline = Pipeline()

pipeline.insert(source, sink, link_map = {sink.snks["x"]: source.srcs["a"]})

pipeline.run()
```

Now you would see the word "hello" printed 11 times.  The 11th time the Frame
is marked as EOS, which means end of stream.  The sink class checks the data it
has gotten and marks the pad as EOS.  When all sink element sink pads are at
EOS the pipeline stops running (in this case there is just one sink element
with one sink pad).

