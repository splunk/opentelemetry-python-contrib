# Tested with weaviate-client==3.26.7
# Code is adapted from official documentation.
# V3 documentation: https://weaviate.io/developers/weaviate/client-libraries/python/python_v3
# Some parts were also adapted from:
# https://towardsdatascience.com/getting-started-with-weaviate-python-client-e85d14f19e4f
import os

import weaviate
import weaviate.classes as wvc

# Load environment variables
from dotenv import load_dotenv

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)

load_dotenv()

CLASS_NAME = "Article"
RAW_QUERY = """
 {
   Get {
     Article(limit: 2) {
        author
        text
     }
   }
 }
 """

# Set up the tracer provider
tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)

# Add OTLP exporter (reads from OTEL_EXPORTER_OTLP_ENDPOINT env var)
otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
    headers=(),
)
otlp_processor = BatchSpanProcessor(otlp_exporter)
tracer_provider.add_span_processor(otlp_processor)

# Add console exporter to see traces in terminal as well
console_exporter = ConsoleSpanExporter()
console_processor = BatchSpanProcessor(console_exporter)
tracer_provider.add_span_processor(console_processor)

# Now instrument Weaviate
WeaviateInstrumentor().instrument()


def create_schema(client):
    client.collections.create(
        name=CLASS_NAME,
        description="An Article class to store a text",
        vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_ollama(
            api_endpoint="http://ollama:11434"
        ),
        generative_config=wvc.config.Configure.Generative.ollama(
            api_endpoint="http://ollama:11434"
        ),
        properties=[
            wvc.config.Property(
                name="author",
                data_type=wvc.config.DataType.TEXT,
                description="The name of the author",
            ),
            wvc.config.Property(
                name="text",
                data_type=wvc.config.DataType.TEXT,
                description="The text content",
            ),
        ],
    )


def get_collection(client):
    """Get the collection to test connection"""
    return client.collections.get(CLASS_NAME)


def delete_collection(client):
    client.collections.delete(CLASS_NAME)


def create_object(collection):
    return collection.data.insert(
        {
            "author": "Robert",
            "text": "Once upon a time, someone wrote a book...",
        }
    )


def create_batch(collection):
    objs = [
        {
            "author": "Robert",
            "text": "Once upon a time, R. wrote a book...",
        },
        {
            "author": "Johnson",
            "text": "Once upon a time, J. wrote some news...",
        },
        {
            "author": "Maverick",
            "text": "Never again, M. will write a book...",
        },
        {
            "author": "Wilson",
            "text": "Lost in the island, W. did not write anything...",
        },
        {
            "author": "Ludwig",
            "text": "As king, he ruled...",
        },
    ]
    with collection.batch.dynamic() as batch:
        for obj in objs:
            batch.add_object(properties=obj)


def query_get(collection):
    return collection.query.fetch_objects(
        return_properties=[
            "author",
        ]
    )


def query_aggregate(collection):
    return collection.aggregate.over_all(total_count=True)


def query_raw(client):
    return client.graphql_raw_query(RAW_QUERY)


def query_near_text(collection, text):
    """Query using nearText to find similar articles."""
    query_result = collection.query.near_text(
        query=text,
        limit=2,
        return_metadata=weaviate.classes.query.MetadataQuery(distance=True),
    )

    return query_result


def validate(client, uuid=None):
    return client.data_object.validate(
        data_object={
            "author": "Robert",
            "text": "Once upon a time, someone wrote a book...",
        },
        uuid=uuid,
        class_name=CLASS_NAME,
    )


def create_schemas(client):
    client.collections.create_from_dict(
        {
            "class": "Author",
            "description": "An author that writes an article",
            "properties": [
                {
                    "name": "name",
                    "dataType": ["string"],
                    "description": "The name of the author",
                },
            ],
        },
    )
    client.collections.create_from_dict(
        {
            "class": CLASS_NAME,
            "description": "An Article class to store a text",
            "properties": [
                {
                    "name": "author",
                    "dataType": ["Author"],
                    "description": "The author",
                },
                {
                    "name": "text",
                    "dataType": ["text"],
                    "description": "The text content",
                },
            ],
        },
    )


def delete_all(client):
    client.collections.delete_all()


def example_schema_workflow(client):
    delete_all(client)

    create_schema(client)
    print("Created schema")
    collection = get_collection(client)
    print("Retrieved collection: ", collection.name)

    uuid = create_object(collection)
    print("Created object of UUID: ", uuid)
    obj = collection.query.fetch_object_by_id(uuid)
    print("Retrieved obj: ", obj)

    create_batch(collection)
    result = query_get(collection)
    print("Query result:", result)
    aggregate_result = query_aggregate(collection)
    print("Aggregate result:", aggregate_result)
    raw_result = query_raw(client)
    print("Raw result: ", raw_result)
    near_text_result = query_near_text(collection, "book")
    print("Near text result: ", near_text_result)

    delete_collection(client)
    print("Deleted schema")


def example_schema_workflow2(client):
    delete_all(client)
    create_schemas(client)


if __name__ == "__main__":
    print("OpenTelemetry Weaviate instrumentation initialized")

    additional_headers = {}
    if (key := os.getenv("COHERE_API_KEY")) is not None:
        additional_headers.update({"X-Cohere-Api-Key": key})
    elif (key := os.getenv("OPENAI_API_KEY")) is not None:
        additional_headers.update({"X-OpenAI-Api-Key": key})
    else:
        print("Warning: No API key found for Cohere or OpenAI")

    if (cluster_name := os.getenv("WEAVIATE_CLUSTER_URL")) is not None:
        auth_config = weaviate.auth.AuthApiKey(
            api_key=os.environ["WEAVIATE_API_KEY"]
        )
        client = weaviate.Client(
            url=os.getenv("WEAVIATE_CLUSTER_URL"),
            auth_client_secret=auth_config,
            timeout_config=(5, 15),
            additional_headers=additional_headers,
        )
    else:
        client = weaviate.connect_to_local(headers=additional_headers)
    print("Client connected")

    try:
        example_schema_workflow2(client)
        example_schema_workflow(client)
        delete_all(client)
    finally:
        # Ensure all spans are exported before exiting
        tracer_provider.force_flush(timeout_millis=5000)
