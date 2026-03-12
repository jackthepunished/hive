"""
BigQuery Tool - Execute SQL queries and explore datasets in Google BigQuery.

Supports:
- Service account authentication via GOOGLE_APPLICATION_CREDENTIALS
- Application Default Credentials (ADC) fallback

Safety features:
- Read-only queries only (INSERT, UPDATE, DELETE, etc. are blocked)
- Configurable row limits to prevent large result sets
- Bytes processed returned for cost awareness
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any

from fastmcp import FastMCP

if TYPE_CHECKING:
    from aden_tools.credentials import CredentialStoreAdapter


# SQL keywords that indicate write operations (case-insensitive)
WRITE_KEYWORDS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bCREATE\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
    r"\bMERGE\b",
    r"\bREPLACE\b",
]

# Compiled regex pattern for detecting write operations
WRITE_PATTERN = re.compile("|".join(WRITE_KEYWORDS), re.IGNORECASE)


def _is_read_only_query(sql: str) -> bool:
    """
    Check if a SQL query is read-only.

    Args:
        sql: The SQL query string to check

    Returns:
        True if the query appears to be read-only, False otherwise
    """
    # Remove comments (both -- and /* */ style)
    sql_no_comments = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    sql_no_comments = re.sub(r"/\*.*?\*/", "", sql_no_comments, flags=re.DOTALL)

    # Check for write keywords
    return not bool(WRITE_PATTERN.search(sql_no_comments))


def _format_schema(schema: list) -> list[dict[str, str]]:
    """Format BigQuery schema fields to simple dictionaries."""
    return [
        {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
        }
        for field in schema
    ]


def _import_bigquery() -> Any:
    """Import google.cloud.bigquery with a consistent error message."""
    try:
        from google.cloud import bigquery
    except ImportError:
        raise ImportError(
            "google-cloud-bigquery is required for BigQuery tools. "
            "Install it with: pip install google-cloud-bigquery"
        ) from None
    return bigquery


def _create_bigquery_client(project_id: str | None = None) -> Any:
    """
    Create a BigQuery client with appropriate credentials.

    Args:
        project_id: Optional project ID override

    Returns:
        BigQuery client instance

    Raises:
        ImportError: If google-cloud-bigquery is not installed
        Exception: If authentication fails
    """
    bigquery = _import_bigquery()

    # Create client - will use ADC if GOOGLE_APPLICATION_CREDENTIALS not set
    if project_id:
        return bigquery.Client(project=project_id)
    return bigquery.Client()


def _create_resource_manager_client() -> Any:
    """Create a Resource Manager client for project discovery."""
    try:
        from google.cloud import resourcemanager_v3
    except ImportError:
        raise ImportError(
            "google-cloud-resource-manager is required for bigquery_list_projects. "
            "Install it with: pip install google-cloud-resource-manager"
        ) from None

    return resourcemanager_v3.ProjectsClient()


def _validate_non_empty(value: str, field_name: str) -> dict[str, str] | None:
    """Validate that a required string argument is non-empty."""
    if not value or not value.strip():
        return {"error": f"{field_name} is required"}
    return None


def _validate_limit(
    value: int,
    *,
    field_name: str,
    minimum: int = 1,
    maximum: int,
) -> dict[str, str] | None:
    """Validate an integer limit argument."""
    if value < minimum:
        return {"error": f"{field_name} must be at least {minimum}"}
    if value > maximum:
        return {
            "error": f"{field_name} cannot exceed {maximum}",
            "help": f"Reduce {field_name} to {maximum} or lower.",
        }
    return None


def _to_iso(value: Any) -> str | None:
    """Convert datetime-like values to ISO strings when possible."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _format_table_ref(table: Any) -> str:
    """Format a table-like object to project.dataset.table when possible."""
    project = getattr(table, "project", None)
    dataset_id = getattr(table, "dataset_id", None)
    table_id = getattr(table, "table_id", None)
    if project and dataset_id and table_id:
        return f"{project}.{dataset_id}.{table_id}"

    reference = getattr(table, "reference", None)
    if reference is not None:
        ref_project = getattr(reference, "project", None)
        ref_dataset = getattr(reference, "dataset_id", None)
        ref_table = getattr(reference, "table_id", None)
        if ref_project and ref_dataset and ref_table:
            return f"{ref_project}.{ref_dataset}.{ref_table}"

    path = getattr(table, "path", None)
    if path:
        return str(path)

    full_table_id = getattr(table, "full_table_id", None)
    if full_table_id:
        return str(full_table_id).replace(":", ".")

    return ""


def _format_partitioning(table: Any) -> dict[str, Any] | None:
    """Extract partitioning metadata from a table if present."""
    partitioning = getattr(table, "time_partitioning", None)
    if partitioning is not None:
        return {
            "type": getattr(partitioning, "type_", None),
            "field": getattr(partitioning, "field", None),
            "expiration_ms": getattr(partitioning, "expiration_ms", None),
        }

    range_partitioning = getattr(table, "range_partitioning", None)
    if range_partitioning is not None:
        range_ = getattr(range_partitioning, "range_", None)
        return {
            "type": "RANGE",
            "field": getattr(range_partitioning, "field", None),
            "range": {
                "start": getattr(range_, "start", None),
                "end": getattr(range_, "end", None),
                "interval": getattr(range_, "interval", None),
            },
        }

    return None


def _format_table_metadata(table: Any) -> dict[str, Any]:
    """Format detailed table metadata into a stable dict."""
    return {
        "table_id": getattr(table, "table_id", None),
        "dataset_id": getattr(table, "dataset_id", None),
        "project_id": getattr(table, "project", None),
        "full_table_id": _format_table_ref(table),
        "type": getattr(table, "table_type", None),
        "description": getattr(table, "description", None),
        "friendly_name": getattr(table, "friendly_name", None),
        "row_count": getattr(table, "num_rows", None),
        "size_bytes": getattr(table, "num_bytes", None),
        "created": _to_iso(getattr(table, "created", None)),
        "modified": _to_iso(getattr(table, "modified", None)),
        "expires": _to_iso(getattr(table, "expires", None)),
        "location": getattr(table, "location", None),
        "labels": getattr(table, "labels", None),
        "partitioning": _format_partitioning(table),
        "clustering": getattr(table, "clustering_fields", None),
        "schema": _format_schema(getattr(table, "schema", []) or []),
    }


def _format_job(job: Any) -> dict[str, Any]:
    """Format a BigQuery job into a compact response shape."""
    destination = getattr(job, "destination", None)
    return {
        "job_id": getattr(job, "job_id", None),
        "project_id": getattr(job, "project", None),
        "location": getattr(job, "location", None),
        "job_type": getattr(job, "job_type", None),
        "state": getattr(job, "state", None),
        "created": _to_iso(getattr(job, "created", None)),
        "started": _to_iso(getattr(job, "started", None)),
        "ended": _to_iso(getattr(job, "ended", None)),
        "user_email": getattr(job, "user_email", None),
        "statement_type": getattr(job, "statement_type", None),
        "bytes_processed": getattr(job, "total_bytes_processed", None),
        "destination": _format_table_ref(destination) if destination is not None else None,
        "error_result": getattr(job, "error_result", None),
        "errors": getattr(job, "errors", None),
    }


def _format_bigquery_error(
    exc: Exception,
    *,
    action: str,
    not_found_help: str | None = None,
    permission_help: str | None = None,
) -> dict[str, str]:
    """Convert BigQuery/Google client exceptions to user-friendly error dicts."""
    if isinstance(exc, ImportError):
        error = {"error": str(exc)}
        if "google-cloud-bigquery" in str(exc):
            error["help"] = "Install the dependency by running: pip install google-cloud-bigquery"
        elif "google-cloud-resource-manager" in str(exc):
            error["help"] = (
                "Install the optional dependency by running: "
                "pip install google-cloud-resource-manager"
            )
        return error

    error_msg = str(exc)
    if (
        "Could not automatically determine credentials" in error_msg
        or "default credentials were not found" in error_msg.lower()
    ):
        return {
            "error": "BigQuery authentication failed",
            "help": "Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path, "
            "or run 'gcloud auth application-default login' for local development.",
        }
    if "Permission" in error_msg and "denied" in error_msg.lower():
        return {
            "error": f"BigQuery permission denied: {error_msg}",
            "help": permission_help
            or "Ensure your service account has the required BigQuery viewer permissions.",
        }
    if "Not found" in error_msg:
        return {
            "error": f"BigQuery resource not found: {error_msg}",
            "help": not_found_help
            or "Check that the referenced project, dataset, table, or job exists.",
        }

    return {"error": f"{action} failed: {error_msg}"}


def register_tools(
    mcp: FastMCP,
    credentials: CredentialStoreAdapter | None = None,
) -> None:
    """Register BigQuery tools with the MCP server."""

    def _get_credentials() -> dict[str, str | None]:
        """Get BigQuery credentials from credential store or environment."""
        if credentials is not None:
            try:
                creds_path = credentials.get("bigquery")
            except KeyError:
                creds_path = None
            try:
                project = credentials.get("bigquery_project")
            except KeyError:
                project = None
            return {
                "credentials_path": creds_path,
                "project_id": project,
            }
        return {
            "credentials_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
        }

    def _get_client(project_id: str | None = None) -> Any:
        """
        Get a BigQuery client with credentials resolution.

        Args:
            project_id: Optional project ID override

        Returns:
            BigQuery client instance
        """
        creds = _get_credentials()
        effective_project = project_id or creds["project_id"]

        # Set credentials path in environment if provided from credential store
        credentials_path = creds.get("credentials_path")
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        return _create_bigquery_client(effective_project)

    @mcp.tool()
    def run_bigquery_query(
        sql: str,
        project_id: str | None = None,
        max_rows: int = 1000,
    ) -> dict:
        """
        Execute a read-only SQL query against Google BigQuery.

        This tool executes SQL queries and returns the results as structured data.
        Only SELECT queries are allowed - write operations (INSERT, UPDATE, DELETE,
        DROP, CREATE, ALTER, TRUNCATE, MERGE) are blocked for safety.
        """
        if not _is_read_only_query(sql):
            return {
                "error": "Write operations are not allowed",
                "help": "Only SELECT queries are permitted. "
                "INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, and MERGE are blocked.",
            }

        limit_error = _validate_limit(max_rows, field_name="max_rows", maximum=10000)
        if limit_error:
            if max_rows > 10000:
                limit_error["help"] = (
                    "For larger result sets, consider using pagination or exporting "
                    "to Cloud Storage."
                )
            return limit_error

        try:
            client = _get_client(project_id)

            query_job = client.query(sql)
            results = query_job.result()

            rows = []
            for i, row in enumerate(results):
                if i >= max_rows:
                    break
                rows.append(dict(row.items()))

            total_rows = results.total_rows
            query_truncated = total_rows > max_rows if total_rows else False

            return {
                "success": True,
                "rows": rows,
                "total_rows": total_rows,
                "rows_returned": len(rows),
                "schema": _format_schema(results.schema),
                "bytes_processed": query_job.total_bytes_processed or 0,
                "query_truncated": query_truncated,
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="BigQuery query",
                not_found_help="Check that the project, dataset, and table names are correct.",
                permission_help="Ensure your service account has the 'BigQuery Data Viewer' "
                "and 'BigQuery Job User' roles.",
            )

    @mcp.tool()
    def describe_dataset(
        dataset_id: str,
        project_id: str | None = None,
    ) -> dict:
        """
        Describe a BigQuery dataset, listing its tables and their schemas.

        Use this tool to explore dataset structure before writing queries.
        Returns table names, types, row counts, and column definitions.
        """
        dataset_error = _validate_non_empty(dataset_id, "dataset_id")
        if dataset_error:
            return dataset_error

        try:
            client = _get_client(project_id)
            dataset_ref = client.dataset(dataset_id)
            tables_list = list(client.list_tables(dataset_ref))

            tables_info = []
            for table_item in tables_list:
                table = client.get_table(table_item.reference)
                tables_info.append(
                    {
                        "table_id": table.table_id,
                        "type": table.table_type,
                        "row_count": table.num_rows,
                        "size_bytes": table.num_bytes,
                        "columns": _format_schema(table.schema) if table.schema else [],
                    }
                )

            return {
                "success": True,
                "dataset_id": dataset_id,
                "project_id": client.project,
                "tables": tables_info,
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="Describe dataset",
                not_found_help=(
                    f"Check that dataset '{dataset_id}' exists and you have access to it."
                ),
                permission_help="Ensure your service account has the 'BigQuery Data Viewer' role.",
            )

    @mcp.tool()
    def bigquery_list_datasets(
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """
        List available datasets in a BigQuery project.

        Use this to discover what data is available before drilling into tables.
        """
        try:
            client = _get_client(project_id)
            datasets = []
            for dataset in client.list_datasets():
                datasets.append(
                    {
                        "dataset_id": getattr(dataset, "dataset_id", None),
                        "project_id": getattr(dataset, "project", None) or client.project,
                        "full_dataset_id": (
                            f"{getattr(dataset, 'project', None) or client.project}."
                            f"{getattr(dataset, 'dataset_id', '')}"
                        ),
                        "friendly_name": getattr(dataset, "friendly_name", None),
                    }
                )

            return {
                "success": True,
                "project_id": client.project,
                "datasets": datasets,
                "count": len(datasets),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="List datasets",
                permission_help="Ensure your service account has permission to view datasets "
                "in the selected project.",
            )

    @mcp.tool()
    def bigquery_list_tables(
        dataset_id: str,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """
        List tables in a BigQuery dataset without fetching full schemas.

        Use this for lightweight discovery before requesting detailed table metadata.
        """
        dataset_error = _validate_non_empty(dataset_id, "dataset_id")
        if dataset_error:
            return dataset_error

        try:
            client = _get_client(project_id)
            dataset_ref = client.dataset(dataset_id)
            tables = []
            for table in client.list_tables(dataset_ref):
                tables.append(
                    {
                        "table_id": getattr(table, "table_id", None),
                        "dataset_id": getattr(table, "dataset_id", dataset_id),
                        "project_id": getattr(table, "project", None) or client.project,
                        "full_table_id": _format_table_ref(table),
                        "type": getattr(table, "table_type", None),
                    }
                )

            return {
                "success": True,
                "project_id": client.project,
                "dataset_id": dataset_id,
                "tables": tables,
                "count": len(tables),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="List tables",
                not_found_help=f"Check that dataset '{dataset_id}' exists in the selected project.",
                permission_help="Ensure your service account has the 'BigQuery Data Viewer' role.",
            )

    @mcp.tool()
    def bigquery_get_table_info(
        dataset_id: str,
        table_id: str,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Get detailed metadata for a single BigQuery table.

        Returns schema, size, row count, partitioning, clustering, and basic metadata.
        """
        dataset_error = _validate_non_empty(dataset_id, "dataset_id")
        if dataset_error:
            return dataset_error
        table_error = _validate_non_empty(table_id, "table_id")
        if table_error:
            return table_error

        try:
            client = _get_client(project_id)
            table_ref = client.dataset(dataset_id).table(table_id)
            table = client.get_table(table_ref)

            return {
                "success": True,
                "table": _format_table_metadata(table),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="Get table info",
                not_found_help=(
                    f"Check that table '{dataset_id}.{table_id}' exists in the selected project."
                ),
                permission_help=(
                    "Ensure your service account has permission to read table metadata."
                ),
            )

    @mcp.tool()
    def bigquery_preview_table(
        dataset_id: str,
        table_id: str,
        project_id: str | None = None,
        max_rows: int = 20,
    ) -> dict[str, Any]:
        """
        Preview rows from a BigQuery table without writing SQL.

        Use this to inspect data shape before querying.
        """
        dataset_error = _validate_non_empty(dataset_id, "dataset_id")
        if dataset_error:
            return dataset_error
        table_error = _validate_non_empty(table_id, "table_id")
        if table_error:
            return table_error
        limit_error = _validate_limit(max_rows, field_name="max_rows", maximum=1000)
        if limit_error:
            return limit_error

        try:
            client = _get_client(project_id)
            table_ref = client.dataset(dataset_id).table(table_id)
            table = client.get_table(table_ref)
            rows_iter = client.list_rows(table, max_results=max_rows)

            rows = [dict(row.items()) for row in rows_iter]
            return {
                "success": True,
                "project_id": client.project,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "rows": rows,
                "rows_returned": len(rows),
                "schema": _format_schema(getattr(table, "schema", []) or []),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="Preview table",
                not_found_help=(
                    f"Check that table '{dataset_id}.{table_id}' exists in the selected project."
                ),
                permission_help="Ensure your service account can preview rows from the table.",
            )

    @mcp.tool()
    def bigquery_dry_run(
        sql: str,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Validate a read-only SQL query and estimate bytes processed without executing it.

        Use this before running expensive queries.
        """
        if not _is_read_only_query(sql):
            return {
                "error": "Write operations are not allowed",
                "help": "Only read-only queries can be dry-run. "
                "INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, and MERGE are blocked.",
            }

        try:
            bigquery = _import_bigquery()
            client = _get_client(project_id)
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = client.query(sql, job_config=job_config)

            return {
                "success": True,
                "project_id": client.project,
                "query_valid": True,
                "estimated_bytes_processed": query_job.total_bytes_processed or 0,
                "job_id": getattr(query_job, "job_id", None),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="Dry run query",
                not_found_help="Check that the query references existing datasets and tables.",
                permission_help="Ensure your service account has the 'BigQuery Job User' role "
                "and read access to referenced tables.",
            )

    @mcp.tool()
    def bigquery_list_jobs(
        project_id: str | None = None,
        max_results: int = 20,
    ) -> dict[str, Any]:
        """
        List recent BigQuery jobs for monitoring and debugging.

        Returns compact job status information including state and bytes processed.
        """
        limit_error = _validate_limit(max_results, field_name="max_results", maximum=100)
        if limit_error:
            return limit_error

        try:
            client = _get_client(project_id)
            jobs = [_format_job(job) for job in client.list_jobs(max_results=max_results)]
            return {
                "success": True,
                "project_id": client.project,
                "jobs": jobs,
                "count": len(jobs),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="List jobs",
                permission_help="Ensure your service account has permission to view BigQuery jobs.",
            )

    @mcp.tool()
    def bigquery_get_job(
        job_id: str,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Get detailed status information for a specific BigQuery job.

        Use this after running a query to inspect job state or failures.
        """
        job_error = _validate_non_empty(job_id, "job_id")
        if job_error:
            return job_error

        try:
            client = _get_client(project_id)
            job = client.get_job(job_id)
            return {
                "success": True,
                "job": _format_job(job),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="Get job",
                not_found_help=f"Check that job '{job_id}' exists and is visible in the project.",
                permission_help=(
                    "Ensure your service account has permission to inspect BigQuery jobs."
                ),
            )

    @mcp.tool()
    def bigquery_list_projects() -> dict[str, Any]:
        """
        List accessible Google Cloud projects for initial BigQuery orientation.

        This uses the Google Cloud Resource Manager client when available.
        """
        try:
            creds = _get_credentials()
            credentials_path = creds.get("credentials_path")
            if credentials_path:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

            client = _create_resource_manager_client()
            projects = []
            for project in client.search_projects():
                projects.append(
                    {
                        "project_id": getattr(project, "project_id", None),
                        "display_name": getattr(project, "display_name", None),
                        "state": getattr(getattr(project, "state", None), "name", None)
                        or str(getattr(project, "state", "")),
                        "name": getattr(project, "name", None),
                        "parent": getattr(project, "parent", None),
                    }
                )

            return {
                "success": True,
                "projects": projects,
                "count": len(projects),
            }
        except Exception as e:
            return _format_bigquery_error(
                e,
                action="List projects",
                permission_help="Ensure your credentials can access Google Cloud Resource Manager "
                "project listings.",
            )
