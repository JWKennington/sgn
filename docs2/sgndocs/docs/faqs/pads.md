# All you need to know about pads and names

Pads are hashable and they also have string names (though that name is not used as the hash).  When developing you might get a bit turned around about how to access and reference pads by name.  Here are a few rules:

- Elements have a notion of a short pad name.  These are verbatim what get passed to `source_pad_names` and `sink_pad_names`. 
- The Element base classes will initialize pads with long pad names of the form `<element name>:["sink" | "source"]:<short name>`.
- These long names are almost never needed for anything programmatically but they can be handy to print out because they carry extra information encoded in the name.
- Usually you will use helper attributes to reference pads by their short names or to look up a pad's short name.

Below is a bit of interactive python code that should be all you need to sort this out.

```python
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
>>> # The pad's long name
>>> print (p.name)
example:src:alice
>>> # A reverse dictionary mapping a pad to a short name
>>> print (e.rsrcs[p])
alice
```
