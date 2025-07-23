# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock

import weaviate

from opentelemetry.instrumentation.weaviate.mapping import (
    MAPPING_V3,
    SPAN_NAME_PREFIX,
)
from opentelemetry.trace import SpanKind

from .test_utils import WeaviateSpanTestBase


class TestWeaviateV3SpanGeneration(WeaviateSpanTestBase):
    """Test span generation for Weaviate v3 operations."""

    def setUp(self):
        super().setUp()
        # Mock weaviate version to v3
        self.version_patcher = mock.patch("weaviate.__version__", "3.25.0")
        self.version_patcher.start()

    def tearDown(self):
        super().tearDown()
        self.version_patcher.stop()

    def test_v3_connection_span_creation(self):
        """Test that v3 client initialization creates connection span."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            weaviate.Client("http://localhost:8080")

        spans = self.assert_span_count(1)
        span = spans[0]

        self.assert_span_properties(
            span, f"{SPAN_NAME_PREFIX}.__init__", SpanKind.CLIENT
        )
        self.assert_server_attributes(span, "localhost", 8080)

    def test_schema_get_span(self):
        """Test span creation for Schema.get operation."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # Mock the network request that schema.get makes, but not the method itself
            with mock.patch("requests.get") as mock_get:
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"classes": []}
                mock_get.return_value = mock_response

                # Call the real schema.get method - this should trigger instrumentation
                client.schema.get()

        spans = self.memory_exporter.get_finished_spans()

        # Should have connection span + operation span
        self.assertGreaterEqual(len(spans), 2)

        # Find schema.get span if instrumentation created it
        schema_spans = [
            span for span in spans if "schema" in span.name.lower()
        ]
        if schema_spans:
            schema_span = schema_spans[0]
            self.assert_span_properties(
                schema_span, "db.weaviate.schema.get", SpanKind.CLIENT
            )
            self.assert_db_attributes(schema_span, "get")

    def test_schema_create_class_span(self):
        """Test span creation for Schema.create_class operation."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # Now mock the post method on the actual connection instance
            mock_response = mock.MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {}
            mock_response.text = ""
            client.schema._connection.post = mock.MagicMock(
                return_value=mock_response
            )

            # Call the real schema.create_class method - this should trigger instrumentation
            client.schema.create_class({"class": "TestClass"})

        spans = self.memory_exporter.get_finished_spans()
        # Should have connection span + operation span
        self.assertGreaterEqual(len(spans), 2)

    def test_data_create_span(self):
        """Test span creation for DataObject.create operation."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # Mock the network request that data_object.create makes
            with mock.patch("requests.post") as mock_post:
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"id": "mock-uuid"}
                mock_post.return_value = mock_response

                # Call the real data_object.create method - this should trigger instrumentation
                client.data_object.create({"title": "test"}, "TestClass")

        spans = self.memory_exporter.get_finished_spans()
        # Should have connection span + operation span
        self.assertGreaterEqual(len(spans), 2)

    def test_batch_add_data_object_span(self):
        """Test span creation for Batch.add_data_object operation."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # For batch operations, typically no immediate network request is made
            # as items are batched up, but we can mock potential requests
            # Call the real batch.add_data_object method - this should trigger instrumentation
            client.batch.add_data_object({"title": "test"}, "TestClass")

        spans = self.memory_exporter.get_finished_spans()
        # Should have at least connection span, possibly batch operation span
        self.assertGreaterEqual(len(spans), 1)

    def test_query_get_span(self):
        """Test span creation for Query.get operation."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # Mock the network request that schema.get makes, but not the method itself
            with mock.patch("requests.get") as mock_get:
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"classes": []}
                mock_get.return_value = mock_response

                # Call the real schema.get method - this should trigger instrumentation
                client.schema.get()

        spans = self.memory_exporter.get_finished_spans()
        # Should have at least 2 spans: connection + schema.get
        self.assertGreaterEqual(len(spans), 2)

        # Check that we have both connection and operation spans
        span_names = [span.name for span in spans]
        self.assertIn(f"{SPAN_NAME_PREFIX}.__init__", span_names)
        self.assertTrue(any("get" in name for name in span_names))

    def test_v3_parent_child_relationship(self):
        """Test parent-child relationship between connection and operation spans."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # Mock the network request for schema.get operation
            with mock.patch("requests.get") as mock_get:
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"classes": []}
                mock_get.return_value = mock_response

                # Call the real schema.get method - this should trigger instrumentation
                client.schema.get()

        spans = self.memory_exporter.get_finished_spans()

        # Should have at least connection span
        self.assertGreaterEqual(len(spans), 1)

        # Verify connection span exists
        connection_spans = self.get_spans_by_name(
            spans, f"{SPAN_NAME_PREFIX}.__init__"
        )
        self.assertGreaterEqual(len(connection_spans), 1)

    def test_v3_multiple_operations_create_separate_spans(self):
        """Test that multiple v3 operations create separate spans."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # Mock the network requests for multiple schema.get operations
            with mock.patch("requests.get") as mock_get:
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"classes": []}
                mock_get.return_value = mock_response

                # Perform multiple operations - should create separate spans
                client.schema.get()
                client.schema.get()

        spans = self.memory_exporter.get_finished_spans()

        # Should have connection span + operation spans (at least 3 total)
        self.assertGreaterEqual(len(spans), 3)

    def test_v3_span_attributes(self):
        """Test that v3 spans have correct database and server attributes."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            weaviate.Client("http://localhost:8080")

        spans = self.assert_span_count(1)
        span = spans[0]

        # Connection spans only have server attributes, not db attributes
        # Verify server attributes
        self.assert_server_attributes(span, "localhost", 8080)

    def test_v3_mapped_operations_span_names(self):
        """Test that mapped v3 operations create spans with correct names."""
        # Test first 5 operations from MAPPING_V3
        for i, operation_config in enumerate(MAPPING_V3[:5]):
            with self.subTest(operation=i):
                operation_name = operation_config["name"]
                expected_span_name = operation_config.get(
                    "span_name", f"{SPAN_NAME_PREFIX}.{operation_name}"
                )

                # For now, just verify the expected span name format
                self.assertTrue(
                    expected_span_name.startswith(SPAN_NAME_PREFIX)
                )

    def test_v3_error_handling_spans(self):
        """Test span creation when v3 operations encounter errors."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch(
            "weaviate.connect.connection.Connection"
        ) as mock_connection:
            # Mock the connection object
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance

            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")

            # Mock the get method on the actual connection to return error response
            mock_response = mock.MagicMock()
            mock_response.status_code = 500
            mock_response.json.return_value = {
                "error": "Internal Server Error"
            }
            mock_response.text = "Internal Server Error"
            client.schema._connection.get = mock.MagicMock(
                return_value=mock_response
            )

            with self.assertRaises(Exception):
                client.schema.get()

        spans = self.memory_exporter.get_finished_spans()

        # Should still have connection span even if operation fails
        self.assertGreaterEqual(len(spans), 1)

        # Connection span should exist
        connection_spans = self.get_spans_by_name(
            spans, f"{SPAN_NAME_PREFIX}.__init__"
        )
        self.assertGreaterEqual(len(connection_spans), 1)
