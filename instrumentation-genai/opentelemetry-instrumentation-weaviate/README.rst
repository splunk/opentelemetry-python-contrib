OpenTelemetry Weaviate Instrumentation
====================================

|pypi|

.. |pypi| image:: https://badge.fury.io/py/opentelemetry-instrumentation-weaviate.svg
   :target: https://pypi.org/project/opentelemetry-instrumentation-weaviate/

This library allows tracing vector database operations made by the
`Weaviate Python Client <https://pypi.org/project/weaviate-client/>`_. It also captures
the duration of the operations and usage metrics.

Installation
------------

If your application is already instrumented with OpenTelemetry, add this
package to your requirements.
::

    pip install opentelemetry-instrumentation-weaviate

Usage
-----

.. code:: python

    from weaviate import Client
    from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor

    WeaviateInstrumentor().instrument()

    client = Client("http://localhost:8080")
    # Your Weaviate operations will now be traced

Configuration
~~~~~~~~~~~~~

You can configure the instrumentation using environment variables:

- ``OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT``: Set to ``true`` to capture query content and results (default: ``false``)

Supported Operations
~~~~~~~~~~~~~~~~~~~~

This instrumentation supports tracing the following Weaviate client operations:

# TODO: Update this to the actual operations supported by the Weaviate client

API
---