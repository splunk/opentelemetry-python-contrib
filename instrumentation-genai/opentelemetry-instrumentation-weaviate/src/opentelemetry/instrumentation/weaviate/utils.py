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

import logging
import traceback
from typing import Any, Callable, Optional, Tuple
from urllib.parse import urlparse

# TODO: get semconv for vector databases
# from opentelemetry.semconv._incubating.attributes import gen_ai_attributes as GenAI

logger = logging.getLogger(__name__)


def dont_throw(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator that catches and logs exceptions, rather than re-raising them,
    to avoid interfering with user code if instrumentation fails.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(
                "OpenTelemetry instrumentation for Weaviate encountered an error in %s: %s",
                func.__name__,
                traceback.format_exc(),
            )
            from opentelemetry.instrumentation.weaviate.config import Config

            if Config.exception_logger:
                Config.exception_logger(e)
            return None

    return wrapper


def parse_url_to_host_port(url: str) -> Tuple[Optional[str], Optional[int]]:
    parsed = urlparse(url)
    host: Optional[str] = parsed.hostname
    port: Optional[int] = parsed.port
    return host, port


def extract_db_operation_name(
    wrapped: Any, module_name: str, function_name: str
) -> str:
    """
    Dynamically extract a database operation name from the Weaviate function call.
    Maps Weaviate operations to standard database operation names.
    """
    # Get the actual function name
    actual_function_name = getattr(wrapped, "__name__", function_name)

    # Extract operation from module and function names
    if "collections" in module_name:
        if any(
            op in actual_function_name.lower()
            for op in ["create", "create_from_dict"]
        ):
            return "create"
        elif any(
            op in actual_function_name.lower()
            for op in ["delete", "delete_all"]
        ):
            return "delete"
        elif any(
            op in actual_function_name.lower()
            for op in ["insert", "add_object"]
        ):
            return "insert"
        elif any(
            op in actual_function_name.lower() for op in ["update", "replace"]
        ):
            return "update"
        elif any(
            op in actual_function_name.lower()
            for op in ["get", "fetch", "query"]
        ):
            return "select"

    # GraphQL operations are typically queries/selects
    if "graphql" in module_name.lower() or "gql" in module_name.lower():
        return "select"

    # Query operations
    if (
        "query" in module_name.lower()
        or "query" in actual_function_name.lower()
    ):
        return "select"

    # Batch operations are typically inserts
    if "batch" in module_name.lower():
        return "insert"

    # Connection/executor operations
    if "connect" in module_name.lower() or "executor" in module_name.lower():
        return "exec"

    # Default fallback based on common function name patterns
    if any(
        op in actual_function_name.lower() for op in ["create", "add", "new"]
    ):
        return "create"
    elif any(
        op in actual_function_name.lower()
        for op in ["delete", "remove", "drop"]
    ):
        return "delete"
    elif any(
        op in actual_function_name.lower() for op in ["insert", "put", "save"]
    ):
        return "insert"
    elif any(
        op in actual_function_name.lower()
        for op in ["update", "modify", "replace", "patch"]
    ):
        return "update"
    elif any(
        op in actual_function_name.lower()
        for op in ["get", "fetch", "find", "search", "query", "select", "read"]
    ):
        return "select"

    # Ultimate fallback
    return "exec"


def extract_collection_name(
    wrapped: Any,
    instance: Any,
    args: Any,
    kwargs: Any,
    module_name: str,
    function_name: str,
) -> Optional[str]:
    """
    Extract collection name from Weaviate function calls.

    Args:
        wrapped: The wrapped function
        instance: The instance object (if any)
        args: Function arguments
        kwargs: Function keyword arguments
        module_name: The module name from mapping
        function_name: The function name from mapping

    Returns:
        Collection name if found, None otherwise
    """
    collection_name = None

    try:
        # Weaviate Client V4 stores this in the "request" attribute of the kwargs
        if (
            kwargs
            and "request" in kwargs
            and hasattr(kwargs["request"], "collection")
        ):
            collection_name = kwargs["request"].collection

        # Check if the instance has a collection attribute
        # TODO: Check V3
        elif hasattr(instance, "_collection"):
            if hasattr(instance._collection, "_name"):
                collection_name = instance._collection._name
            elif hasattr(instance._collection, "name"):
                collection_name = instance._collection.name

        return collection_name

    except Exception:
        # Silently ignore any errors during extraction to avoid breaking the tracing

        pass

    return None
