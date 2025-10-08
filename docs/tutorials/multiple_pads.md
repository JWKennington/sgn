# Multiple Pads

What if we want more than one pad?  It is possible to have many source and sink
pads on an element. SGN provides basic bookkeeping utilities for you, but
generally what the "correct" behavior is is up to you. Lets try a more complicated
example with multiple pads

```
  ┌─────────────────────────────────────────┐
  │        Source Element 1                 │
  │                                         │
  │  [ source pad 'a' ]  [ source pad 'b' ] │
  └──────────┬────────────────────┬─────────┘
             │                    │
             │  Frame             │  Frame
             ▼                    ▼
  ┌──────────┴────────────────────┴─────────┐
  │  [ sink pad 'x' ]    [ sink pad 'y' ]   │
  │                                         │
  │        Sink Element 1                   │
  └─────────────────────────────────────────┘
```

```python
#!/usr/bin/env python3

from dataclasses import dataclass
from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

@dataclass
class MySourceClass(SourceElement):
    # Of the form {"pad name": <data to put on the pad}
    pad_str_map: dict=None
    def __post_init__(self):
        # We will just use pad_str_map to define the source pad names too
        self.source_pad_names = tuple(self.pad_str_map)
        super().__post_init__()
        # save a pad map also hashed by pad not the string
        # NOTE: this must be done after super() post init so that the source pads exist
        self.pad_map = {self.srcs[p]: d for p,d in self.pad_str_map.items()}
        self.cnt = 0
    def new(self, pad):
        self.cnt += 1
        return Frame(data=self.pad_map[pad], EOS=self.cnt > 10)

class MySinkClass(SinkElement):
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        print (frame.data)

source = MySourceClass(pad_str_map = {"a": "Hello!", "b":"How are you?"})
sink = MySinkClass(sink_pad_names = ("x","y"))

pipeline = Pipeline()

pipeline.insert(source, sink, link_map = {sink.snks["x"]: source.srcs["a"], sink.snks["y"]: source.srcs["b"],})

pipeline.run()
```

Running this produces the following output:

```
e1-056827:~ crh184$ ./sgn-readme
Hello!
How are you?
Hello!
How are you?
Hello!
How are you?
Hello!
How are you?
Hello!
How are you?
Hello!
How are you?
```

Note that the total number of outputs is 12.  We had the counter in the new()
method which is a pad dependent method. It will be called once for each pad
during each loop iteration.  What if we wanted 10 loop iterations before
sending EOS? There is a convenient "internal" pad inside of every element that
is guaranteed to be called before any source pads and after any sink pads.
Lets modify the code to use that.
