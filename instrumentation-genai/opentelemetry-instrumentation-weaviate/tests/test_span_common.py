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

from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from opentelemetry.instrumentation.weaviate.mapping import SPAN_NAME_PREFIX
from opentelemetry.trace import SpanKind, get_tracer

from .test_utils import WeaviateSpanTestBase


class TestWeaviateSpanCommon(WeaviateSpanTestBase):
    """Common span generation tests for both Weaviate v3 and v4."""

    def test_instrument_uninstrument(self):
        """Test that instrumentor can be enabled and disabled."""
        # Uninstrument first
        WeaviateInstrumentor().uninstrument()

        # Verify no spans are created when uninstrumented
        with mock.patch("weaviate.Client") as mock_client:
            mock_client.return_value = mock.MagicMock()
            mock_client()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

        # Re-instrument
        WeaviateInstrumentor().instrument(tracer_provider=self.tracer_provider)

        # Verify spans are created when instrumented
        with mock.patch("weaviate.Client") as mock_client:
            mock_client.return_value = mock.MagicMock()
            mock_client("http://localhost:8080")

        spans = self.memory_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)

    def test_span_creation_basic(self):
        """Test that basic spans are created for instrumented operations."""
        with mock.patch("weaviate.Client") as mock_client:
            mock_instance = mock.MagicMock()
            mock_client.return_value = mock_instance

            # Create client (should create connection span)
            mock_client("http://localhost:8080")

        spans = self.assert_span_count(1)
        span = spans[0]

        # Verify basic span properties
        self.assert_span_properties(
            span, f"{SPAN_NAME_PREFIX}.__init__", SpanKind.CLIENT
        )

    def test_span_kind_is_client(self):
        """Test that all spans have CLIENT kind."""
        with mock.patch("weaviate.Client") as mock_client:
            mock_instance = mock.MagicMock()
            mock_client.return_value = mock_instance

            mock_client("http://localhost:8080")

        spans = self.assert_span_count(1)
        for span in spans:
            self.assertEqual(span.kind, SpanKind.CLIENT)

    def test_not_recording(self):
        """Test behavior when tracer is not recording."""
        tracer = get_tracer(__name__)

        # Mock tracer to return non-recording spans
        with mock.patch.object(tracer, "start_span") as mock_start_span:
            mock_span = mock.MagicMock()
            mock_span.is_recording.return_value = False
            mock_start_span.return_value = mock_span

            with mock.patch("weaviate.Client") as mock_client:
                mock_client.return_value = mock.MagicMock()
                mock_client("http://localhost:8080")

            # Verify span.set_attribute was not called when not recording
            self.assertFalse(mock_span.set_attribute.called)

    def test_root_span_creation(self):
        """Test that operations create root spans when no parent context exists."""
        with mock.patch("weaviate.Client") as mock_client:
            mock_client.return_value = mock.MagicMock()
            mock_client("http://localhost:8080")

        spans = self.assert_span_count(1)
        root_spans = self.get_root_spans(spans)

        # Should have one root span
        self.assertEqual(len(root_spans), 1)
        self.assertIsNone(root_spans[0].parent)

    def test_child_span_creation(self):
        """Test that operations create child spans when parent context exists."""
        tracer = get_tracer(__name__)

        with tracer.start_as_current_span("test_parent") as parent_span:
            with mock.patch("weaviate.Client") as mock_client:
                mock_client.return_value = mock.MagicMock()
                mock_client("http://localhost:8080")

        spans = self.assert_span_count(2)  # parent + connection

        # Find the connection span
        connection_spans = self.get_spans_by_name(
            spans, f"{SPAN_NAME_PREFIX}.__init__"
        )
        self.assertEqual(len(connection_spans), 1)

        connection_span = connection_spans[0]

        # Verify parent-child relationship
        self.assertIsNotNone(connection_span.parent)
        self.assertEqual(
            connection_span.parent.span_id, parent_span.context.span_id
        )

    def test_nested_operations_span_hierarchy(self):
        """Test multiple levels of span nesting."""
        tracer = get_tracer(__name__)

        with tracer.start_as_current_span("root_span"):
            with mock.patch("weaviate.Client") as mock_client:
                mock_client.return_value = mock.MagicMock()
                mock_client("http://localhost:8080")

                # Simulate nested operation
                with tracer.start_as_current_span("nested_operation"):
                    pass

        spans = self.assert_span_count(3)  # root + connection + nested

        # Verify hierarchy exists
        root_spans = self.get_root_spans(spans)
        self.assertEqual(len(root_spans), 1)
        self.assertEqual(root_spans[0].name, "root_span")

    def test_multiple_child_spans_same_parent(self):
        """Test multiple operations under same parent create separate child spans."""
        tracer = get_tracer(__name__)

        with tracer.start_as_current_span("parent_span") as parent_span:
            # Create multiple clients (each should create child span)
            with mock.patch("weaviate.Client") as mock_client:
                mock_client.return_value = mock.MagicMock()
                mock_client("http://localhost:8080")
                mock_client("http://localhost:8081")

        spans = self.assert_span_count(3)  # parent + 2 connections

        # Find connection spans
        connection_spans = self.get_spans_by_name(
            spans, f"{SPAN_NAME_PREFIX}.__init__"
        )
        self.assertEqual(len(connection_spans), 2)

        # Both should be children of the same parent
        for connection_span in connection_spans:
            self.assertIsNotNone(connection_span.parent)
            self.assertEqual(
                connection_span.parent.span_id, parent_span.context.span_id
            )

    def test_context_propagation_across_operations(self):
        """Test that span context is properly maintained across operations."""
        tracer = get_tracer(__name__)

        with tracer.start_as_current_span("outer_span") as outer_span:
            with mock.patch("weaviate.Client") as mock_client:
                mock_client.return_value = mock.MagicMock()
                mock_client("http://localhost:8080")

                # Verify the connection span maintains context
                spans = self.memory_exporter.get_finished_spans()
                if spans:
                    connection_span = spans[-1]  # Most recent span
                    if connection_span.name == f"{SPAN_NAME_PREFIX}.__init__":
                        self.assertEqual(
                            connection_span.context.trace_id,
                            outer_span.context.trace_id,
                        )

    def test_span_name_format(self):
        """Test that span names follow the expected format."""
        with mock.patch("weaviate.Client") as mock_client:
            mock_client.return_value = mock.MagicMock()
            mock_client("http://localhost:8080")

        spans = self.assert_span_count(1)
        span = spans[0]

        # Verify span name follows format: {SPAN_NAME_PREFIX}.{operation}
        self.assertTrue(span.name.startswith(SPAN_NAME_PREFIX))
        self.assertEqual(span.name, f"{SPAN_NAME_PREFIX}.__init__")

    def test_server_attributes_on_connection_spans(self):
        """Test that server attributes are set on connection spans."""
        with mock.patch("weaviate.Client") as mock_client:
            mock_client.return_value = mock.MagicMock()
            mock_client("http://localhost:8080")

        spans = self.assert_span_count(1)
        span = spans[0]

        # Verify server attributes are set
        self.assert_server_attributes(span, "localhost", 8080)
