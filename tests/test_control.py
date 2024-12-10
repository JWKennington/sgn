"""Tests for control module."""

import json
from sgn.control import (
    HTTPControl,
    HTTPControlSourceElement,
    HTTPControlTransformElement,
    HTTPControlSinkElement,
)

import http.client
import urllib.parse


def standard_library_post(url, data=None, json_data=None, headers=None):
    """
    Mimics the functionality of requests.post() using only the Python standard library.

    Args:
        url (str): The full URL to send the POST request to.
        data (dict or None): The form data to include in the request body (optional).
        json_data (dict or None): The JSON data to include in the request body
        (optional).
        headers (dict or None): Custom headers to include in the request (optional).

    Returns:
        dict: A dictionary containing the status, reason, and response body.
    """
    # Parse the URL
    parsed_url = urllib.parse.urlparse(url)
    conn = (
        http.client.HTTPSConnection(parsed_url.netloc)
        if parsed_url.scheme == "https"
        else http.client.HTTPConnection(parsed_url.netloc)
    )

    # Prepare the request body
    if json_data:
        body = json.dumps(json_data)
        content_type = "application/json"
    elif data:
        body = urllib.parse.urlencode(data)
        content_type = "application/x-www-form-urlencoded"
    else:
        body = None
        content_type = None

    # Set headers
    headers = headers or {}
    if content_type and "Content-Type" not in headers:
        headers["Content-Type"] = content_type

    # Send the request
    conn.request("POST", parsed_url.path or "/", body=body, headers=headers)

    # Get the response
    response = conn.getresponse()
    response_body = response.read().decode("utf-8")

    # Close the connection
    conn.close()

    return {
        "status": response.status,
        "reason": response.reason,
        "body": response_body,
    }


def standard_library_get(url, params=None, headers=None):
    """
    Mimics the functionality of requests.get() using only the Python standard library.

    Args:
        url (str): The full URL to send the GET request to.
        params (dict or None): Query parameters to include in the URL (optional).
        headers (dict or None): Custom headers to include in the request (optional).

    Returns:
        dict: A dictionary containing the status, reason, and response body.
    """
    # Parse the URL
    parsed_url = urllib.parse.urlparse(url)

    # Add query parameters to the URL if provided
    if params:
        query_string = urllib.parse.urlencode(params)
        path = (
            f"{parsed_url.path}?{query_string}"
            if parsed_url.path
            else f"/?{query_string}"
        )
    else:
        path = parsed_url.path or "/"

    # Choose the appropriate connection based on the scheme
    conn = (
        http.client.HTTPSConnection(parsed_url.netloc)
        if parsed_url.scheme == "https"
        else http.client.HTTPConnection(parsed_url.netloc)
    )

    # Send the GET request
    conn.request("GET", path, headers=headers or {})

    # Get the response
    response = conn.getresponse()
    response_body = response.read().decode("utf-8")

    # Close the connection
    conn.close()

    return {
        "status": response.status,
        "reason": response.reason,
        "body": response_body,
    }


class TestHTTPControlElements:
    """Tests for HTTPControlSourceElement, HTTPTransformElement and
    HTTPControlSinkElement classes."""

    def test_init_and_context_manager(self):
        src = HTTPControlSourceElement(
            name="testsrc",
            source_pad_names=[
                "blah",
            ],
        )
        src.get_queues["testsrc"].put({"test": "test"})
        trans = HTTPControlTransformElement(
            name="testtrans",
            source_pad_names=[
                "blah",
            ],
            sink_pad_names=[
                "blah",
            ],
        )
        trans.get_queues["testtrans"].put({"test": "test"})
        sink = HTTPControlSinkElement(
            name="testsink",
            sink_pad_names=[
                "blah",
            ],
        )
        sink.get_queues["testsink"].put({"test": "test"})
        with HTTPControl() as control:
            # UGH this is super annoying but if you don't wait a while then the
            # bottle server might not be ready.
            import time

            time.sleep(3)
            if control.http_thread.is_alive():
                for elem in ("testsrc", "testtrans", "testink"):
                    standard_library_post(
                        f"http://{control.host}:{control.port}/post/{elem}",
                        json_data=json.dumps({"a": "b"}),
                    )
                    standard_library_post(
                        f"http://{control.host}:{control.port}/post/{elem}",
                        json_data=None,
                    )
                    standard_library_get(
                        f"http://{control.host}:{control.port}/get/{elem}"
                    )
