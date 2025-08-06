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

import os
from typing import List

import weaviate

from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import Span


class WeaviateSpanTestBase(TestBase):
    """Base class for all Weaviate span generation tests"""

    def setUp(self):
        super().setUp()
        self.cassette_path = os.path.join(
            os.path.dirname(__file__), "cassettes/"
        )
        self.version = self.get_weaviate_version()
        WeaviateInstrumentor().instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        super().tearDown()
        WeaviateInstrumentor().uninstrument()

    def get_weaviate_version(self):
        # Try to get the version from the weaviate module or client
        try:
            return int(weaviate.__version__.split(".")[0])
        except AttributeError:
            # Fallback: try to get from client instance if available
            client = weaviate.Client("http://localhost:8080")
            return int(client.get_version().split(".")[0])

    def get_weaviate_client(self):
        """Get a Weaviate client instance."""
        if self.get_weaviate_version() >= 4:
            return weaviate.connect_to_local(skip_init_checks=True)
        return weaviate.Client("http://localhost:8080")

    def get_basic_call(self, client):
        """Perform a basic operation to generate spans."""
        # This can be any operation that generates a span, e.g., getting schema
        if self.get_weaviate_version() >= 4:
            return client.collections.get("TestCollection")
        else:
            return client.schema.get()

    def assert_span_count(self, count: int) -> List[Span]:
        """Assert that the memory exporter has the expected number of spans."""
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), count)
        return spans

    def assert_parent_child_relationship(
        self, parent_span: Span, child_span: Span
    ):
        """Assert that child_span is a child of parent_span."""
        self.assertIsNotNone(child_span.parent)
        self.assertEqual(
            child_span.parent.span_id, parent_span.context.span_id
        )
        self.assertEqual(
            child_span.parent.trace_id, parent_span.context.trace_id
        )
