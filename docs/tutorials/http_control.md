# Using HTTP Control in SGN Pipelines

This tutorial explains how to use the HTTP control capabilities in SGN for monitoring and controlling pipeline elements via HTTP interfaces.

## Overview

SGN provides classes in the `control.py` module that allow you to expose your pipeline elements via an HTTP interface. This enables:

- Remote monitoring of element state
- Dynamic reconfiguration of elements during runtime
- Integration with external systems through HTTP

The main classes available are:

- `HTTPControl`: A context manager for managing the HTTP server
- `HTTPControlSourceElement`: A source element with HTTP control capabilities
- `HTTPControlTransformElement`: A transform element with HTTP control capabilities
- `HTTPControlSinkElement`: A sink element with HTTP control capabilities

## Basic Usage

Let's start with a simple example that demonstrates how to use `HTTPControl` with an SGN pipeline:

```{.python notest}
#!/usr/bin/env python3

from sgn.apps import Pipeline
from sgn.control import HTTPControl
from sgn.base import SourceElement, SinkElement, Frame

class MySource(SourceElement):
    def new(self, pad):
        return Frame(data="Hello from Source", EOS=True)

class MySink(SinkElement):
    def pull(self, pad, frame):
        print(f"Received: {frame.data}")

source = MySource(source_pad_names=("out",))
sink = MySink(sink_pad_names=("in",))

pipeline = Pipeline()
pipeline.insert(source, sink, link_map={sink.snks["in"]: source.srcs["out"]})

# Run the pipeline with HTTP control
with HTTPControl() as control:
    pipeline.run()
```

When you run this example, an HTTP server will start on the default port (8080), and you'll see a message indicating the server address. The pipeline will run as usual, but now with an HTTP server available.

## HTTP-Controllable Elements

Now let's create a more advanced example using the HTTP-controllable element classes:

```{.python notest}
#!/usr/bin/env python3

import time
from sgn.apps import Pipeline
from sgn.control import HTTPControl, HTTPControlSourceElement, HTTPControlSinkElement
from sgn.base import Frame

class ControlledSource(HTTPControlSourceElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.state = {"message": "Hello, World!", "count": 0}

    def new(self, pad):
        # Update state from HTTP if available
        time.sleep(1)
        HTTPControl.exchange_state(self.name, self.state)

        # Increment counter and return frame
        self.state["count"] += 1
        return Frame(data=f"{self.state['message']} #{self.state['count']}", EOS=self.signaled_eos())

class ControlledSink(HTTPControlSinkElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.received_count = 0
        self.last_message = ""

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        self.received_count += 1
        self.last_message = frame.data
        print(f"Sink received: {frame.data}")

        # Update state for HTTP clients
        HTTPControl.exchange_state(
            self.name,
            {"received_count": self.received_count, "last_message": self.last_message}
        )

# Create pipeline elements
source = ControlledSource(name="source", source_pad_names=("out",))
sink = ControlledSink(name="sink", sink_pad_names=("in",))

# Create and connect pipeline
pipeline = Pipeline()
pipeline.insert(source, sink, link_map={sink.snks["in"]: source.srcs["out"]})

# Set custom port if needed
HTTPControl.port = 8080  # Default is already 8080

# Run with HTTP control
with HTTPControl() as control:
    pipeline.run()
```

## Interacting with HTTP Endpoints

Once your pipeline is running with HTTP control, you can interact with it using HTTP requests. Here are some examples using `curl`:

### Get Element State

To retrieve the current state of an element:

```bash
curl http://localhost:8080/get/source
```

This will return a JSON object with the element's state, for example:

```json
{"message": "Hello, World!", "count": 42}
```

You can also request a specific property:

```bash
curl http://localhost:8080/get/source/message
```

Response:
```
"Hello, World!"
```

### Update Element State

To update an element's state:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"message": "Updated message"}' http://localhost:8080/post/source
```

This will update the `message` property of the source element to "Updated message".

## Advanced Example: Adding Transforms with HTTP Control

Let's extend our example to include a transform element with HTTP control:

```{.python notest}
#!/usr/bin/env python3

import time
from sgn.apps import Pipeline
from sgn.control import HTTPControl, HTTPControlSourceElement, HTTPControlTransformElement, HTTPControlSinkElement
from sgn.base import Frame

class ControlledSource(HTTPControlSourceElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.state = {"message": "Hello, World!", "count": 0}

    def new(self, pad):
        # Update state from HTTP if available
        time.sleep(1)
        HTTPControl.exchange_state(self.name, self.state)

        # Increment counter and return frame
        self.state["count"] += 1
        return Frame(data=f"{self.state['message']} #{self.state['count']}", EOS=self.signaled_eos())

class ControlledTransform(HTTPControlTransformElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.state = {"prefix": "Transformed: ", "active": True}
        self.current_frame = None

    def pull(self, pad, frame):
        # Store the incoming frame
        self.current_frame = frame

        # Update state from HTTP if available
        HTTPControl.exchange_state(self.name, self.state)

    def new(self, pad):
        # Check if we have a frame to process
        if self.current_frame:
            # Apply transformation if active
            if self.state['active']:
                return Frame(
                    data=f"{self.state['prefix']}{self.current_frame.data}",
                    EOS=self.current_frame.EOS
                )
            else:
                # Pass through without transformation
                return Frame(
                    data=self.current_frame.data,
                    EOS=self.current_frame.EOS
                )

        # Fallback if no frame is available
        return Frame(data="No input available")

class ControlledSink(HTTPControlSinkElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.received_count = 0

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        self.received_count += 1
        print(f"Received ({self.received_count}): {frame.data}")
        HTTPControl.exchange_state(self.name, {"received_count": self.received_count})

# Create pipeline elements
source = ControlledSource(name="source", source_pad_names=("out",))
transform = ControlledTransform(
    name="transform",
    sink_pad_names=("in",),
    source_pad_names=("out",)
)
sink = ControlledSink(name="sink", sink_pad_names=("in",))

# Create and connect pipeline
pipeline = Pipeline()
pipeline.insert(
    source, transform, sink,
    link_map={
        transform.snks["in"]: source.srcs["out"],
        sink.snks["in"]: transform.srcs["out"]
    }
)

# Run with HTTP control
with HTTPControl() as control:
    # This writes the HTTP endpoint to registry.txt
    pipeline.run()
```

With this pipeline, you can:

1. Change the source message
2. Enable/disable the transform
3. Change the transform prefix
4. Monitor how many frames the sink has received

```
$ curl -X POST -H "Content-Type: application/json" -d '{"active": false}' http://localhost:8080/post/transform
$ curl -X POST -H "Content-Type: application/json" -d '{"active": true}' http://localhost:8080/post/transform
```

## Customizing HTTP Control

You can customize how `HTTPControl` works by setting class attributes before using it:

```{.python notest}
# Set custom host and port
HTTPControl.host = "0.0.0.0"  # Listen on all interfaces
HTTPControl.port = 9090       # Use port 9090 instead of default 8080

# Set a custom registry file
HTTPControl.registry_file = "my_registry.txt"

# Set a URL path prefix/tag
HTTPControl.tag = "my_app"  # URLs will be /my_app/get/... instead of /get/...

# Then use it as usual
with HTTPControl() as control:
    pipeline.run()
```

With the tag set, your endpoints would be accessible at paths like:
```
http://localhost:9090/my_app/get/source
http://localhost:9090/my_app/post/transform
```

## Performance Considerations

- The HTTP control mechanisms add overhead to your pipeline
- For high-throughput applications, consider reducing how frequently you call `exchange_state()`
- Queue size is fixed at 1 by default, which means only the latest data is kept

## Conclusion

The HTTP control capabilities in SGN provide a powerful way to monitor and control your pipeline elements during runtime. This is particularly useful for:

- Debugging and monitoring
- Building interactive applications
- Integrating with dashboards or other visualization tools
- Remote configuration of long-running pipelines

By exposing your pipeline elements via HTTP, you make your SGN application more flexible and easier to integrate with other systems.
