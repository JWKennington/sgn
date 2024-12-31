import json
import socket
from dataclasses import dataclass
from queue import Queue
from threading import Thread

from sgn import SinkElement, SourceElement, TransformElement
from sgn.base import SGN_LOG_LEVELS, get_sgn_logger
from sgn.bottle import Bottle, request, response, run  # type: ignore
from sgn.sources import SignalEOS

LOGGER = get_sgn_logger("control", SGN_LOG_LEVELS)


# Define a function to run the Bottle app in a separate thread
def run_bottle_app(post_queues=None, get_queues=None, host="localhost", port=8080):
    """A function that sets up post and get queues for a bottle server running
    on host:port.

    post_queues is a dictionary whose keys define routes of the form
    http://host:port/post/key. The values are Queue objects where the data that is
    posted by the external request will be stored.

    get_queues define the inverse, e.g., http://host:port/get/key and the Queue
    values are where the data for the request comes from.
    """
    app = Bottle()

    for postroute, postqueue in post_queues.items():

        def post(postqueue=postqueue):
            data = request.json  # Get JSON payload
            if data:
                # Drain the post queue
                while not postqueue.empty():
                    postqueue.get()
                    postqueue.task_done()
                # Then put in the data we have
                postqueue.join()
                postqueue.put(data)  # Put JSON payload into the queue
                return {"status": "success", "message": "Data received"}
            else:
                return {"status": "error", "message": "Invalid JSON"}

        app.route("/post/%s" % postroute, method="POST", callback=post)

    for getroute, getqueue in get_queues.items():

        def get(getqueue=getqueue):
            data = {}
            # Get the last data in the queue
            while not getqueue.empty():
                data = getqueue.get()
                getqueue.task_done()
            # Put a copy back in
            getqueue.join()
            getqueue.put(data)
            response.content_type = "application/json"
            return json.dumps(data)

        app.route("/get/%s" % getroute, method="GET", callback=get)

    run(app, host=host, port=port, debug=True)


class HTTPControl(SignalEOS):
    """A context manager that stores a bottle app running in a separate thread.

    If you have a pipeline called p do,

    with HTTPControl() as control:
        p.run()

    The bottle process is started and stoped on enter and exit. This class
    inherits SignalEOS context manager actions too, becuase otherwise bottle will
    respond to ctrl+C and generally you want to deal with both signals and bottle
    contexts in a coherent way when executing a pipeline, so this implementation
    tries to do that.
    """

    port = 8080
    host = socket.gethostbyname("localhost")
    post_queues: dict[str, Queue] = {}
    get_queues: dict[str, Queue] = {}
    http_thread = None

    def __enter__(self):
        # The bottle thread doesn't want to die without daemon mode (which
        # doesn't kill it, it just lets the program die) FIXME
        HTTPControl.http_thread = Thread(
            target=run_bottle_app,
            kwargs={
                "post_queues": HTTPControl.post_queues,
                "get_queues": HTTPControl.get_queues,
                "port": HTTPControl.port,
                "host": HTTPControl.host,
            },
            daemon=True,
        )
        HTTPControl.http_thread.start()  # Start the Bottle app as a subthread
        LOGGER.info(
            "Bottle app running on http://%s:%s", HTTPControl.host, HTTPControl.port
        )
        super().__enter__()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        HTTPControl.http_thread.join(3.0)  # Wait for the subthread to clean up
        super().__exit__(exc_type, exc_value, exc_traceback)

    @classmethod
    def exchange_state(cls, name, state_dict):
        """Automate the common task of reading and writing state to an element.
        name is the name of an element (which is the key of both get and post queues)
        and state_dict is a dictionary of state variables with **correct** types that
        can be coerced out of json.  This will mostly work out of the box for simple
        data types like ints and floats and strings, but complicated data will probably
        not work.  FIXME consider supporting more complex types if it comes up.

        HTTPControl.exchange_state(<elem name>, state_dict) will drain the post
        queue and update matching keys in statdict with the contents of the postqueue.
        The postqueue is not preserved so if you call it again immediately the
        postqueue is likely to be empty resulting in no change to state_dict.
        """
        while not cls.post_queues[name].empty():
            postdata = cls.post_queues[name].get()
            for k in state_dict:
                if k in postdata:
                    state_dict[k] = type(state_dict[k])(postdata[k])
            cls.post_queues[name].task_done()
        cls.post_queues[name].join()
        # drain the get queue and put data into it
        while not cls.get_queues[name].empty():
            cls.get_queues[name].get()
            cls.get_queues[name].task_done()
        cls.get_queues[name].join()
        cls.get_queues[name].put(state_dict)


@dataclass
class HTTPControlSourceElement(SourceElement, HTTPControl):
    """A lightweight subclass of SourceElement that defaults to setting up post
    and get routes based on the provided element name.  HTTP Queues are limited
    to a size of queuesize of 1. Posts will always succeed by draining the queue
    first.  Gets will preserve the data in the queue"""

    def __post_init__(self):
        SourceElement.__post_init__(self)
        queuesize = 1
        HTTPControl.post_queues[self.name] = Queue(queuesize)
        HTTPControl.get_queues[self.name] = Queue(queuesize)


@dataclass
class HTTPControlTransformElement(TransformElement, HTTPControl):
    """A lightweight subclass of TransformElement that defaults to setting up post
    and get routes based on the provided element name.  HTTP Queues are limited
    to a size of queuesize of 1. Posts will always succeed by draining the queue
    first.  Gets will preserve the data in the queue"""

    def __post_init__(self):
        TransformElement.__post_init__(self)
        queuesize = 1
        HTTPControl.post_queues[self.name] = Queue(queuesize)
        HTTPControl.get_queues[self.name] = Queue(queuesize)


@dataclass
class HTTPControlSinkElement(SinkElement, HTTPControl):
    """A lightweight subclass of SinkElement that defaults to setting up post
    and get routes based on the provided element name.  HTTP Queues are limited
    to a size of queuesize of 1. Posts will always succeed by draining the queue
    first.  Gets will preserve the data in the queue"""

    queuesize: int = 1

    def __post_init__(self):
        SinkElement.__post_init__(self)
        queuesize = 1
        HTTPControl.post_queues[self.name] = Queue(queuesize)
        HTTPControl.get_queues[self.name] = Queue(queuesize)
