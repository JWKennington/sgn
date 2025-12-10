# All you need to know about pads and names

Pads are hashable and they also have string names (though that name is not used as the hash).  When developing you might get a bit turned around about how to access and reference pads by name.  Here are a few rules:

- Elements have a notion of a short pad name.  These are verbatim what get passed to `source_pad_names` and `sink_pad_names`.
- The Element base classes will initialize pads with long pad names of the form `<element name>:["src" | "snk"]:<short name>`.
- These long names are almost never needed for anything programmatically but they can be handy to print out because they carry extra information encoded in the name.
- Usually you will use helper attributes to reference pads by their short names or to look up a pad's short name.

Below is a bit of interactive python code that should be all you need to sort this out.

```{.python notest}
>>> from sgn.base import SourceElement
>>> e = SourceElement(name="example", source_pad_names=("alice","bob"))
>>> # Here are some relevant ways to access pad information
>>> # All of the "short" names -- these will be the strings provided by source_pad_names in the initialization
>>> print (e.source_pad_names)
('alice', 'bob')
>>> # A dictionary mapping the short name to a given pad object, e.g.,
>>> p = e.srcs["alice"]
>>> print (type(p))
<class 'sgn.base.SourcePad'>
>>> # The pad's short name (simplest way to get it)
>>> print (p.pad_name)
alice
>>> # The pad's long/full name
>>> print (p.name)
example:src:alice
>>> # A reverse dictionary mapping a pad to a short name (legacy approach)
>>> print (e.rsrcs[p])
alice
```

!!! tip "Accessing Short Names"
    The `pad.pad_name` attribute is the simplest way to get a pad's short name. The reverse dictionaries (`rsrcs`, `rsnks`) are still available but `pad_name` is more direct.

## Static Pads (Class-Level Configuration)

For reusable element classes, you can define pads at the class level rather than specifying them at instantiation time. This uses class variables:

```{.python notest}
>>> from dataclasses import dataclass
>>> from typing import ClassVar
>>> from sgn.base import TransformElement

>>> @dataclass
... class MyTransform(TransformElement):
...     static_sink_pads: ClassVar[list[str]] = ["input"]
...     static_source_pads: ClassVar[list[str]] = ["output"]
...     allow_dynamic_sink_pads: ClassVar[bool] = False  # Prevent user from adding pads
...     allow_dynamic_source_pads: ClassVar[bool] = False
...
...     def pull(self, pad, frame): pass
...     def new(self, pad): return Frame()

>>> # No need to specify pad names - they're defined at the class level
>>> elem = MyTransform(name="my_elem")
>>> print(elem.sink_pad_names)
['input']
>>> print(elem.source_pad_names)
['output']
```

When `allow_dynamic_*_pads=True` (the default), user-provided pads are combined with static pads:

```{.python notest}
>>> @dataclass
... class FlexibleTransform(TransformElement):
...     static_source_pads: ClassVar[list[str]] = ["monitor"]  # Always present
...     # allow_dynamic_source_pads defaults to True
...
...     def pull(self, pad, frame): pass
...     def new(self, pad): return Frame()

>>> elem = FlexibleTransform(sink_pad_names=["in"], source_pad_names=["out"])
>>> print(elem.source_pad_names)
['out', 'monitor']  # User pads + static pads
```

See the [Base API documentation](../api/base.md#static-pads-class-level-pad-configuration) for more details.
