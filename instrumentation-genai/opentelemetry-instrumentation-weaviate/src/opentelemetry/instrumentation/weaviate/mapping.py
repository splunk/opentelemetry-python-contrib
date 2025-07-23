SPAN_NAME_PREFIX: str = "db.weaviate"

CONNECTION_WRAPPING: list[dict[str, str]] = [
    {"module": "weaviate", "name": "connect_to_local"},
    {"module": "weaviate", "name": "connect_to_weaviate_cloud"},
    {"module": "weaviate", "name": "connect_to_custom"},
]

MAPPING_V3: list[dict[str, str]] = [
    # Schema operations
    {
        "module": "weaviate.schema",
        "name": "Schema.get",
        "span_name": "db.weaviate.schema.get",
    },
    {
        "module": "weaviate.schema",
        "name": "Schema.create_class",
        "span_name": "db.weaviate.schema.create_class",
    },
    {
        "module": "weaviate.schema",
        "name": "Schema.create",
        "span_name": "db.weaviate.schema.create",
    },
    {
        "module": "weaviate.schema",
        "name": "Schema.delete_class",
        "span_name": "db.weaviate.schema.delete_class",
    },
    {
        "module": "weaviate.schema",
        "name": "Schema.delete_all",
        "span_name": "db.weaviate.schema.delete_all",
    },
    # Data CRUD operations
    {
        "module": "weaviate.data.crud_data",
        "name": "DataObject.create",
        "span_name": "db.weaviate.data.crud_data.create",
    },
    {
        "module": "weaviate.data.crud_data",
        "name": "DataObject.validate",
        "span_name": "db.weaviate.data.crud_data.validate",
    },
    {
        "module": "weaviate.data.crud_data",
        "name": "DataObject.get",
        "span_name": "db.weaviate.data.crud_data.get",
    },
    # Batch operations
    {
        "module": "weaviate.batch.crud_batch",
        "name": "Batch.add_data_object",
        "span_name": "db.weaviate.batch.crud_batch.add_data_object",
    },
    {
        "module": "weaviate.batch.crud_batch",
        "name": "Batch.flush",
        "span_name": "db.weaviate.batch.crud_batch.flush",
    },
    # GraphQL query operations
    {
        "module": "weaviate.gql.query",
        "name": "Query.get",
        "span_name": "db.weaviate.gql.query.get",
    },
    {
        "module": "weaviate.gql.query",
        "name": "Query.aggregate",
        "span_name": "db.weaviate.gql.query.aggregate",
    },
    {
        "module": "weaviate.gql.query",
        "name": "Query.raw",
        "span_name": "db.weaviate.gql.query.raw",
    },
    {
        "module": "weaviate.gql.get",
        "name": "GetBuilder.do",
        "span_name": "db.weaviate.gql.query.get.do",
    },
]


MAPPING_V4: list[dict[str, str]] = [
    {
        "module": "weaviate.collections.queries.near_text.query",
        "name": "_NearTextQuery.near_text",
        "span_name": "collections.query.near_text",
    },
    {
        "module": "weaviate.collections.queries.fetch_objects.query",
        "name": "_FetchObjectsQuery.fetch_objects",
        "span_name": "collections.query.fetch_objects",
    },
    {
        "module": "weaviate.collections.grpc.query",
        "name": "_QueryGRPC.get",
        "span_name": "collections.query.get",
    },
    {
        "module": "weaviate.collections.data",
        "name": "_DataCollection.insert",
        "span_name": "collections.data.insert",
    },
    {
        "module": "weaviate.collections.data",
        "name": "_DataCollection.replace",
        "span_name": "collections.data.replace",
    },
    {
        "module": "weaviate.collections.data",
        "name": "_DataCollection.update",
        "span_name": "collections.data.update",
    },
    # Collections
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.get",
        "span_name": "collections.get",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.create",
        "span_name": "collections.create",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.delete",
        "span_name": "collections.delete",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.delete_all",
        "span_name": "collections.delete_all",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.create_from_dict",
        "span_name": "collections.create_from_dict",
    },
    # Batch
    {
        "module": "weaviate.collections.batch.collection",
        "name": "_BatchCollection.add_object",
        "span_name": "collections.batch.add_object",
    },
]
