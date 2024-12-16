# Internal Pads

In the previous example, we noticed that there were 12 lines of output. This
was because new gets called for each pad and the counter was being incremented
for each pad.  If we want to instead count the number of loop iterations, we
can use the internal pad. The internal pad is guaranteed to be called before
any pad's new().

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
    def internal(self, pad):
        self.cnt += 1
    def new(self, pad):
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

Now the output has the expected number of iterations
```
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

We can also use the internal method to make a more useful sink output, e.g.,

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
    def internal(self, pad):
        self.cnt += 1
    def new(self, pad):
        return Frame(data=self.pad_map[pad], EOS=self.cnt > 10)

class MySinkClass(SinkElement):
    def __post_init__(self):
        super().__post_init__()
        self.combined_string = ""
    def internal(self, pad):
        print (self.combined_string)
        self.combined_string = ""
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        self.combined_string += " %s" % frame.data

source = MySourceClass(pad_str_map = {"a": "Hello!", "b":"How are you?"})
sink = MySinkClass(sink_pad_names = ("x","y"))

pipeline = Pipeline()

pipeline.insert(source, sink, link_map = {sink.snks["x"]: source.srcs["a"], sink.snks["y"]: source.srcs["b"],})

pipeline.run()
``` 

which now produces

```
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
 Hello! How are you?
```
