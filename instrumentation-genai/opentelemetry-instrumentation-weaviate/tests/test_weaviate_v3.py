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
import unittest

import vcr
import weaviate

from opentelemetry.instrumentation.weaviate.mapping import (
    SPAN_NAME_PREFIX,
)
from opentelemetry.trace import SpanKind

from .test_utils import WeaviateSpanTestBase


def should_skip_tests():
    version = int(weaviate.__version__.split(".")[0])
    return version != 3


@unittest.skipIf(
    should_skip_tests(), "Skipping Weaviate v3 tests for versions < 4"
)
class TestWeaviateV3SpanGeneration(WeaviateSpanTestBase):
    def test_schema_get_span(self):
        cassette_path = (
            f"{self.cassette_path}test_schema_get_span_v{self.version}.yaml"
        )
        with vcr.use_cassette(cassette_path):
            client = self.get_weaviate_client()
            client.schema.get()
            spans = self.assert_span_count(1)
            span = spans[0]
            self.assert_span_properties(
                span, f"{SPAN_NAME_PREFIX}.schema.get", SpanKind.CLIENT
            )

    def test_server_connection_attributes(self):
        cassette_path = f"{self.cassette_path}test_server_connection_attributes_v{self.version}.yaml"
        with vcr.use_cassette(cassette_path):
            client = self.get_weaviate_client()
            client.schema.get()
            spans = self.assert_span_count(1)
            span = spans[0]
            self.assert_server_attributes(span, "localhost", 8080)

    def test_parent_child_span_relationship(self):
        cassette_path = f"{self.cassette_path}test_parent_child_span_relationship_v{self.version}.yaml"
        with vcr.use_cassette(cassette_path):
            client = self.get_weaviate_client()
            client.schema.get()
            spans = self.assert_span_count(1)
            span = spans[0]
            self.assertIsNone(span.parent)

            # TODO: find a child span to assert against

    def test_distance_attribute(self):
        cassette_path = (
            f"{self.cassette_path}test_distance_attribute_v{self.version}.yaml"
        )
        with vcr.use_cassette(cassette_path):
            client = self.get_weaviate_client()
            near_text_filter = {
                "concepts": ["lost while writing"],
            }

            (
                client.query.get("Article", ["text", "author"])
                .with_additional(["id", "distance"])
                .with_near_text(near_text_filter)
                .do()
            )

            spans = self.assert_span_count(
                2
            )  # Weaviate V3 does a "get" against the schema, then a "do" against the data
            span = spans[1]
            self.assertGreater(len(span.events), 1)
            event = span.events[0]
            self.assertIn(
                "db.weaviate.document.distance", dict(event.attributes)
            )
