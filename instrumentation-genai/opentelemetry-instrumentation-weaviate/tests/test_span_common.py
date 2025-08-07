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

import vcr

from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor

from .test_utils import WeaviateSpanTestBase


class TestWeaviateSpanCommon(WeaviateSpanTestBase):
    """Common span generation tests for both Weaviate v3 and v4."""

    def test_instrument_uninstrument(self):
        """Test that instrumentor can be enabled and disabled."""
        cassette_path = f"{self.cassette_path}test_instrument_uninstrument_v{self.version}.yaml"
        with vcr.use_cassette(cassette_path):
            # Uninstrument first
            WeaviateInstrumentor().uninstrument()

            spans = self.memory_exporter.get_finished_spans()
            self.assertEqual(len(spans), 0)

            # Re-instrument
            WeaviateInstrumentor().instrument(
                tracer_provider=self.tracer_provider
            )
            client = self.get_weaviate_client()
            # Perform an operation to generate spans
            self.get_basic_call(client)

            spans = self.memory_exporter.get_finished_spans()
            self.assertGreater(len(spans), 0)

    def test_span_creation_basic(self):
        """Test that basic spans are created for instrumented operations."""
        cassette_path = f"{self.cassette_path}test_span_creation_basic_v{self.version}.yaml"
        with vcr.use_cassette(cassette_path):
            client = self.get_weaviate_client()
            # Perform an operation to generate spans
            self.get_basic_call(client)
            self.assert_span_count(1)

    # def test_span_kind_is_client(self):
    #     """Test that all spans have CLIENT kind."""
    #     spans = self.assert_span_count(1)
    #     for span in spans:
    #         self.assertEqual(span.kind, SpanKind.CLIENT)
