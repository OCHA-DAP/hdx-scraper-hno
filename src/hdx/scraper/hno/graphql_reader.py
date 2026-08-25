"""GraphQL transport for the Humanitarian Action Fabric GraphQL API.

See docs/decisions/0002-interim-subscription-key-auth.md for why reader/auth
construction is isolated here rather than using
``hdx.pipelineutils.reader.Read.create_readers``'s ``header_auths`` (which
hardcodes the header name to ``Authorization``, not the
``Ocp-Apim-Subscription-Key`` this API currently needs) directly.
"""

import logging
from datetime import datetime
from typing import Any

from hdx.pipelineutils.reader import Read
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)

GRAPHQL_READER_NAME = "hpc_graphql"


class GraphQLError(Exception):
    """Raised when a GraphQL response's top-level errors array is non-empty.

    Fabric can return HTTP 200 alongside a populated errors array, so this
    must be checked explicitly rather than relying on download_json raising on
    HTTP status alone.
    """


def create_readers(
    folder: str,
    saved_dir: str,
    temp_dir: str,
    save: bool,
    use_saved: bool,
    hdx_auth: str,
    hpc_subscription_key: str,
    today: datetime | None,
    rate_limit: dict | None = None,
) -> None:
    """Set up the default/hdx/hpc_graphql readers.

    Mirrors what Read.create_readers does for the default and hdx downloaders,
    but adds hpc_graphql directly via Download.generate_downloaders so it can
    carry the Ocp-Apim-Subscription-Key header (not expressible via
    Read.create_readers' header_auths, which always names the header
    "Authorization") and widen allowed_methods to cover POST, since Fabric's
    GraphQL endpoint only accepts POST and the underlying retry-on-transient-
    error logic defaults to GET only.
    """
    custom_configs = {
        "hdx": {"headers": {"Authorization": hdx_auth}},
        GRAPHQL_READER_NAME: {
            "headers": {"Ocp-Apim-Subscription-Key": hpc_subscription_key},
            "allowed_methods": frozenset(["GET", "POST"]),
        },
    }
    Download.generate_downloaders(custom_configs, rate_limit=rate_limit)
    Read.generate_retrievers(folder, saved_dir, temp_dir, save, use_saved, today=today)


def execute_query(
    url: str, query: str, variables: dict, filename: str
) -> dict[str, Any]:
    """Execute a single GraphQL query and return its "data" payload.

    filename must be distinct per distinct (query, variables) combination:
    every call hits the same POST url, and Retrieve derives its saved/replayed
    fixture filename from the url, so calls sharing a filename would collide
    on save/replay.
    """
    response = Read.get_reader(GRAPHQL_READER_NAME).download_json(
        url,
        filename=filename,
        post=True,
        json_string=True,
        parameters={"query": query, "variables": variables},
    )
    errors = response.get("errors")
    if errors:
        raise GraphQLError(f"{filename}: {errors}")
    return response["data"]


def paginate(
    url: str,
    query: str,
    variables: dict,
    connection_field: str,
    filename_prefix: str,
    first: int = 100,
) -> list[dict]:
    """Page through a cursor-paginated GraphQL connection, returning all items.

    connection_field is the top-level field name of the connection being
    paginated (e.g. "attachmentFacts").
    """
    items = []
    after = None
    page = 1
    while True:
        page_variables = {**variables, "first": first, "after": after}
        data = execute_query(
            url,
            query,
            page_variables,
            filename=f"{filename_prefix}_p{page}.json",
        )
        connection = data[connection_field]
        items.extend(connection["items"])
        if not connection["hasNextPage"]:
            return items
        after = connection["endCursor"]
        page += 1
