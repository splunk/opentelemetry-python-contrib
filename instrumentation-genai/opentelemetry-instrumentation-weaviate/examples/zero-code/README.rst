OpenTelemetry Weaviate-Client Zero-Code Instrumentation Example
======================================================

This is an example of how to instrument weaviate-client with zero code changes,
using `opentelemetry-instrument`.

When :code:`example_v{3,4}.py <main.py>`_ is run, it exports traces to an OTLP-compatible endpoint.
Traces include details such as the span name and other attributes.

Note: :code:`.env <.env>`_ file configures additional environment variables:
- :code:`OTEL_LOGS_EXPORTER=otlp` to specify exporter type.
- :code:`OTEL_EXPORTER_OTLP_ENDPOINT` to specify the endpoint for exporting traces (default is http://localhost:4317).

Setup
-----

Anytime a bash example has {3,4}, choose which version of weaviate-client you mean to use.

Run the docker environment contained in ../docker-compose.yml to start a Weaviate instance and ollama with built in embeddings.

An OTLP compatible endpoint should be listening for traces http://localhost:4317.
If not, update :code:`OTEL_EXPORTER_OTLP_ENDPOINT` as well.

Next, set up a virtual environment like this:

::

    python3 -m venv .venv
    source .venv/bin/activate
    pip install "python-dotenv[cli]"
    pip install -r requirements_v{3,4}.txt

Run
---

Run the example like this:

::

    dotenv run -- opentelemetry-instrument python example_v{3,4}.py

You should see spans around article creation, retrieval, and deletion. There should be an example span on near_text search as well
