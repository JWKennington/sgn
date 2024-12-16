# Hello World

Here is a simple example implementing the above graph

```python
#!/usr/bin/env python3

from sgn.base import SourceElement, SinkElement, Frame
from sgn.apps import Pipeline

class MySourceClass(SourceElement):
    def new(self, pad):
        return Frame(data="hello")

class MySinkClass(SinkElement):
    def pull(self, pad, frame):
        print (frame.data)

source = MySourceClass(source_pad_names = ("a",))
sink = MySinkClass(sink_pad_names = ("x",))

pipeline = Pipeline()

pipeline.insert(source, sink, link_map = {sink.snks["x"]: source.srcs["a"]})

pipeline.run()
```

If you run this, it will run forever and you will see

```
hello
hello
hello
hello
hello
hello
hello
hello
hello
hello
...
```

You would need to send SIG INT or SIG kill to stop the program.
