#Transform Elements

Graphs can have other elements called "transform elements." These have both source and sink pads.  Also, it is possible to connect a source pad to multiple sink pads (but not the other way around). Lets try to implement this graph

```
 ---------------------------------------------
|                                             |
|              Source Element 1               |
|                                             |
 --- [source pad 'a'] --- [source pad 'b'] --- 
           |\                |\
           | \               | \
           |  \              |  \_________________________________________
           |   \_____________|_________________________                   \
           |                 |                         \                   \
           |                 |                          V                   V
           |                 |                 --- [sink pad 'l'  ] --- [sink pad 'm'  ] ---
           |                 |                |                                             |
           |                 |                |            Transform Element 1              |
           |                 |                |                                             |
           |                 |                 ------------- [source pad 'n'] --------------
           |                 |                                   /
           |                 |                                  /
           |                 |                                 /
           |                 |                                /
           |                 |                               /
           | data flow       |                              /
           V                 V                             V
 --- [sink pad 'x'  ] --- [sink pad 'y'  ] --- [sink pad 'z'  ] --- 
|                                                                  | 
|               Sink Element 1                                     |
|                                                                  | 
-------------------------------------------------------------------
```

```python
#!/usr/bin/env python3

from dataclasses import dataclass
from sgn.base import SourceElement, SinkElement, TransformElement, Frame
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
    def internal(self):
        self.cnt += 1
    def new(self, pad):
        return Frame(data=self.pad_map[pad], EOS=self.cnt > 10)

class MyTransformClass(TransformElement):
    def __post_init__(self):
        # written to assume a single source pad
        assert len(self.source_pad_names) == 1
        super().__post_init__()
        self.out_string = ""
        self.out_frame = None
        self.EOS = False
    def pull(self, pad, frame):
        self.out_string += " %s" % frame.data
        self.EOS |= frame.EOS
    def internal(self):
        # Reverse the data for fun.
        self.outframe = Frame(data=self.out_string[::-1], EOS=self.EOS)
        self.out_string = ""
    def new(self, pad):
        # This element just has one source pad
        return self.outframe
        
        
class MySinkClass(SinkElement):
    def __post_init__(self):
        super().__post_init__()
        self.combined_string = ""
    def internal(self):
        print (self.combined_string)
        self.combined_string = ""
    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        self.combined_string += " %s" % frame.data

source = MySourceClass(pad_str_map = {"a": "Hello!", "b":"How are you?"})
transform = MyTransformClass(sink_pad_names = ("l","m",), source_pad_names = ("n",))
sink = MySinkClass(sink_pad_names = ("x","y","z"))

pipeline = Pipeline()

pipeline.insert(source,
               transform, 
               sink,
               link_map = {sink.snks["x"]: source.srcs["a"], 
                           sink.snks["y"]: source.srcs["b"],
                           transform.snks["l"]: source.srcs["a"],
                           transform.snks["m"]: source.srcs["b"],
                           sink.snks["z"]: transform.srcs["n"]
                          }
               )

pipeline.run()
```
which produces

```
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
 Hello! How are you? ?uoy era woH !olleH 
```

