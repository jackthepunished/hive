"""
Tests for BigQuery tool.

Tests cover:
- Query execution with mocked BigQuery client
- Read-only enforcement (blocking write operations)
- Row limiting and preview limits
- Dataset, table, job, and project discovery operations
- Dry-run cost estimation
- Error handling and user-friendly messages
- Credential resolution
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from aden_tools.credentials import CredentialStoreAdapter
from aden_tools.tools.bigquery_tool import register_tools


@pytest.fixture
def mcp():
    """Create a FastMCP instance for testing."""
    return FastMCP("test-server")


@pytest.fixture
def mock_credentials():
    """Create mock credentials for testing."""
    return CredentialStoreAdapter.for_testing(
        {
            "bigquery": "/path/to/service-account.json",
            "bigquery_project": "test-project",
        }
    )


@pytest.fixture
def registered_mcp(mcp, mock_credentials):
    """Register BigQuery tools with mock credentials."""
    register_tools(mcp, credentials=mock_credentials)
    return mcp


def _mock_schema_field(name: str, field_type: str, mode: str = "NULLABLE") -> MagicMock:
    field = MagicMock()
    field.name = name
    field.field_type = field_type
    field.mode = mode
    return field


class TestReadOnlyEnforcement:
    """Tests for SQL write operation blocking."""

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO table VALUES (1, 2)",
            "UPDATE table SET col = 1",
            "DELETE FROM table WHERE id = 1",
            "DROP TABLE my_table",
            "CREATE TABLE my_table (id INT)",
            "ALTER TABLE my_table ADD COLUMN new_col INT",
            "TRUNCATE TABLE my_table",
            "MERGE INTO target USING source ON condition WHEN MATCHED THEN UPDATE",
            "insert into table values (1)",
        ],
    )
    def test_blocks_write_operations(self, registered_mcp, sql):
        tool = registered_mcp._tool_manager._tools["run_bigquery_query"]
        result = tool.fn(sql=sql)
        assert "error" in result
        assert "Write operations are not allowed" in result["error"]

    def test_allows_select(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["run_bigquery_query"]
        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_create_client.side_effect = Exception("Mock error")
            result = tool.fn(sql="SELECT * FROM table")
            assert "Write operations are not allowed" not in result.get("error", "")

    def test_dry_run_blocks_write_operations(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_dry_run"]
        result = tool.fn(sql="DELETE FROM table WHERE id = 1")
        assert "error" in result
        assert "Write operations are not allowed" in result["error"]


class TestQueryExecution:
    """Tests for query execution and dry runs."""

    def test_successful_query(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["run_bigquery_query"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_create_client.return_value = mock_client

            mock_query_job = MagicMock()
            mock_query_job.total_bytes_processed = 1024

            mock_row1 = MagicMock()
            mock_row1.items.return_value = [("id", 1), ("name", "Alice")]
            mock_row2 = MagicMock()
            mock_row2.items.return_value = [("id", 2), ("name", "Bob")]

            mock_results = MagicMock()
            mock_results.total_rows = 2
            mock_results.__iter__ = lambda self: iter([mock_row1, mock_row2])
            mock_results.schema = [
                _mock_schema_field("id", "INTEGER", "REQUIRED"),
                _mock_schema_field("name", "STRING"),
            ]

            mock_query_job.result.return_value = mock_results
            mock_client.query.return_value = mock_query_job

            result = tool.fn(sql="SELECT id, name FROM users")

            assert result["success"] is True
            assert result["rows"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            assert result["rows_returned"] == 2
            assert result["bytes_processed"] == 1024

    def test_query_truncation(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["run_bigquery_query"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_create_client.return_value = mock_client

            mock_query_job = MagicMock()
            mock_query_job.total_bytes_processed = 2048
            mock_rows = []
            for i in range(10):
                row = MagicMock()
                row.items.return_value = [("id", i)]
                mock_rows.append(row)

            mock_results = MagicMock()
            mock_results.total_rows = 10
            mock_results.__iter__ = lambda self: iter(mock_rows)
            mock_results.schema = [_mock_schema_field("id", "INTEGER", "REQUIRED")]

            mock_query_job.result.return_value = mock_results
            mock_client.query.return_value = mock_query_job

            result = tool.fn(sql="SELECT id FROM users", max_rows=5)

            assert result["success"] is True
            assert result["total_rows"] == 10
            assert result["rows_returned"] == 5
            assert result["query_truncated"] is True

    def test_dry_run_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_dry_run"]

        with (
            patch(
                "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
            ) as mock_create_client,
            patch("aden_tools.tools.bigquery_tool.bigquery_tool._import_bigquery") as mock_import,
        ):
            mock_bigquery = MagicMock()
            mock_import.return_value = mock_bigquery

            mock_client = MagicMock()
            mock_client.project = "test-project"
            mock_create_client.return_value = mock_client

            mock_job = MagicMock()
            mock_job.total_bytes_processed = 4096
            mock_job.job_id = "job_123"
            mock_client.query.return_value = mock_job

            result = tool.fn(sql="SELECT * FROM users")

            assert result["success"] is True
            assert result["estimated_bytes_processed"] == 4096
            assert result["job_id"] == "job_123"
            mock_client.query.assert_called_once()
            assert "job_config" in mock_client.query.call_args.kwargs


class TestValidation:
    """Tests for input validation."""

    def test_rejects_invalid_query_max_rows(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["run_bigquery_query"]
        assert "error" in tool.fn(sql="SELECT 1", max_rows=0)
        assert "error" in tool.fn(sql="SELECT 1", max_rows=10001)

    def test_preview_requires_dataset_and_table(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_preview_table"]
        assert "dataset_id is required" in tool.fn(dataset_id="", table_id="users")["error"]
        assert "table_id is required" in tool.fn(dataset_id="analytics", table_id="")["error"]

    def test_preview_rejects_large_max_rows(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_preview_table"]
        result = tool.fn(dataset_id="analytics", table_id="users", max_rows=1001)
        assert "error" in result
        assert "max_rows cannot exceed 1000" in result["error"]

    def test_list_tables_requires_dataset(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_list_tables"]
        result = tool.fn(dataset_id="")
        assert "error" in result
        assert "dataset_id is required" in result["error"]

    def test_get_table_info_requires_args(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_get_table_info"]
        assert "dataset_id is required" in tool.fn(dataset_id="", table_id="users")["error"]
        assert "table_id is required" in tool.fn(dataset_id="analytics", table_id="")["error"]

    def test_get_job_requires_job_id(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_get_job"]
        result = tool.fn(job_id="")
        assert "error" in result
        assert "job_id is required" in result["error"]

    def test_list_jobs_rejects_large_max_results(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_list_jobs"]
        result = tool.fn(max_results=101)
        assert "error" in result
        assert "max_results cannot exceed 100" in result["error"]


class TestDatasetAndTableDiscovery:
    """Tests for dataset and table discovery operations."""

    def test_describe_dataset_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["describe_dataset"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_client.project = "test-project"
            mock_create_client.return_value = mock_client

            mock_table_item = MagicMock()
            mock_table_item.reference = "test-project.analytics.users"
            mock_client.list_tables.return_value = [mock_table_item]

            mock_table = MagicMock()
            mock_table.table_id = "users"
            mock_table.table_type = "TABLE"
            mock_table.num_rows = 1000
            mock_table.num_bytes = 10240
            mock_table.schema = [
                _mock_schema_field("id", "INTEGER", "REQUIRED"),
                _mock_schema_field("email", "STRING"),
            ]
            mock_client.get_table.return_value = mock_table

            result = tool.fn(dataset_id="analytics")

            assert result["success"] is True
            assert result["project_id"] == "test-project"
            assert result["tables"][0]["table_id"] == "users"

    def test_list_datasets_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_list_datasets"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_client.project = "test-project"
            mock_create_client.return_value = mock_client
            mock_client.list_datasets.return_value = [
                SimpleNamespace(
                    project="test-project",
                    dataset_id="analytics",
                    friendly_name="Analytics",
                ),
                SimpleNamespace(project="test-project", dataset_id="sales", friendly_name=None),
            ]

            result = tool.fn()

            assert result["success"] is True
            assert result["count"] == 2
            assert result["datasets"][0]["full_dataset_id"] == "test-project.analytics"

    def test_list_tables_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_list_tables"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_client.project = "test-project"
            mock_create_client.return_value = mock_client
            mock_client.list_tables.return_value = [
                SimpleNamespace(
                    project="test-project",
                    dataset_id="analytics",
                    table_id="users",
                    table_type="TABLE",
                ),
                SimpleNamespace(
                    project="test-project",
                    dataset_id="analytics",
                    table_id="orders_view",
                    table_type="VIEW",
                ),
            ]

            result = tool.fn(dataset_id="analytics")

            assert result["success"] is True
            assert result["count"] == 2
            assert result["tables"][1]["type"] == "VIEW"

    def test_get_table_info_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_get_table_info"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_create_client.return_value = mock_client

            mock_table = MagicMock()
            mock_table.table_id = "users"
            mock_table.dataset_id = "analytics"
            mock_table.project = "test-project"
            mock_table.table_type = "TABLE"
            mock_table.description = "User table"
            mock_table.friendly_name = "Users"
            mock_table.num_rows = 100
            mock_table.num_bytes = 2048
            mock_table.schema = [_mock_schema_field("id", "INTEGER", "REQUIRED")]
            mock_table.labels = {"team": "data"}
            mock_table.clustering_fields = ["country"]
            mock_table.time_partitioning = SimpleNamespace(
                type_="DAY", field="created_at", expiration_ms=None
            )
            mock_table.range_partitioning = None
            mock_table.created = None
            mock_table.modified = None
            mock_table.expires = None
            mock_table.location = "US"
            mock_client.get_table.return_value = mock_table

            result = tool.fn(dataset_id="analytics", table_id="users")

            assert result["success"] is True
            assert result["table"]["full_table_id"] == "test-project.analytics.users"
            assert result["table"]["partitioning"]["type"] == "DAY"
            assert result["table"]["clustering"] == ["country"]

    def test_preview_table_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_preview_table"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_client.project = "test-project"
            mock_create_client.return_value = mock_client

            mock_table = MagicMock()
            mock_table.schema = [
                _mock_schema_field("id", "INTEGER", "REQUIRED"),
                _mock_schema_field("name", "STRING"),
            ]
            mock_client.get_table.return_value = mock_table

            row1 = MagicMock()
            row1.items.return_value = [("id", 1), ("name", "Alice")]
            row2 = MagicMock()
            row2.items.return_value = [("id", 2), ("name", "Bob")]
            mock_client.list_rows.return_value = [row1, row2]

            result = tool.fn(dataset_id="analytics", table_id="users", max_rows=2)

            assert result["success"] is True
            assert result["rows_returned"] == 2
            assert result["rows"][0]["name"] == "Alice"


class TestJobMonitoring:
    """Tests for job discovery and monitoring operations."""

    def test_list_jobs_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_list_jobs"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_client.project = "test-project"
            mock_create_client.return_value = mock_client
            mock_client.list_jobs.return_value = [
                SimpleNamespace(
                    job_id="job_1",
                    project="test-project",
                    location="US",
                    job_type="query",
                    state="DONE",
                    created=None,
                    started=None,
                    ended=None,
                    user_email="bot@example.com",
                    statement_type="SELECT",
                    total_bytes_processed=1024,
                    destination=None,
                    error_result=None,
                    errors=None,
                )
            ]

            result = tool.fn()

            assert result["success"] is True
            assert result["count"] == 1
            assert result["jobs"][0]["job_id"] == "job_1"

    def test_get_job_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_get_job"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_client = MagicMock()
            mock_create_client.return_value = mock_client
            mock_client.get_job.return_value = SimpleNamespace(
                job_id="job_123",
                project="test-project",
                location="US",
                job_type="query",
                state="RUNNING",
                created=None,
                started=None,
                ended=None,
                user_email="bot@example.com",
                statement_type="SELECT",
                total_bytes_processed=5000,
                destination=None,
                error_result=None,
                errors=None,
            )

            result = tool.fn(job_id="job_123")

            assert result["success"] is True
            assert result["job"]["state"] == "RUNNING"
            assert result["job"]["bytes_processed"] == 5000


class TestProjectDiscovery:
    """Tests for GCP project discovery."""

    def test_list_projects_success(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_list_projects"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_resource_manager_client"
        ) as mock_create_rm_client:
            mock_client = MagicMock()
            mock_create_rm_client.return_value = mock_client
            mock_client.search_projects.return_value = [
                SimpleNamespace(
                    project_id="project-1",
                    display_name="Project One",
                    state=SimpleNamespace(name="ACTIVE"),
                    name="projects/111",
                    parent="folders/222",
                )
            ]

            result = tool.fn()

            assert result["success"] is True
            assert result["count"] == 1
            assert result["projects"][0]["project_id"] == "project-1"

    def test_list_projects_missing_dependency(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_list_projects"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_resource_manager_client"
        ) as mock_create_rm_client:
            mock_create_rm_client.side_effect = ImportError(
                "google-cloud-resource-manager is required for bigquery_list_projects. "
                "Install it with: pip install google-cloud-resource-manager"
            )

            result = tool.fn()

            assert "error" in result
            assert "google-cloud-resource-manager" in result["error"]
            assert "help" in result


class TestErrorHandling:
    """Tests for error handling and user-friendly messages."""

    def test_authentication_error(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["run_bigquery_query"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_create_client.side_effect = Exception(
                "Could not automatically determine credentials"
            )
            result = tool.fn(sql="SELECT 1")

            assert "authentication failed" in result["error"].lower()
            assert "GOOGLE_APPLICATION_CREDENTIALS" in result["help"]

    def test_permission_error(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_get_table_info"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_create_client.side_effect = Exception(
                "Permission denied for table project.dataset.table"
            )
            result = tool.fn(dataset_id="analytics", table_id="users")

            assert "permission denied" in result["error"].lower()
            assert "help" in result

    def test_not_found_error(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["bigquery_get_job"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_create_client.side_effect = Exception("Not found: Job job_123 was not found")
            result = tool.fn(job_id="job_123")

            assert "not found" in result["error"].lower()
            assert "help" in result

    def test_dataset_not_found_error(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["describe_dataset"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_create_client.side_effect = Exception(
                "Not found: Dataset project:nonexistent was not found"
            )
            result = tool.fn(dataset_id="nonexistent")

            assert "not found" in result["error"].lower()

    def test_import_error_message(self, registered_mcp):
        tool = registered_mcp._tool_manager._tools["run_bigquery_query"]

        with patch(
            "aden_tools.tools.bigquery_tool.bigquery_tool._create_bigquery_client"
        ) as mock_create_client:
            mock_create_client.side_effect = ImportError(
                "google-cloud-bigquery is required for BigQuery tools. "
                "Install it with: pip install google-cloud-bigquery"
            )
            result = tool.fn(sql="SELECT 1")

            assert "google-cloud-bigquery" in result["error"]
            assert "help" in result


class TestCredentialResolution:
    """Tests for credential resolution from different sources."""

    def test_uses_credential_store(self, mcp):
        mock_creds = CredentialStoreAdapter.for_testing(
            {
                "bigquery": "/custom/path/credentials.json",
                "bigquery_project": "custom-project",
            }
        )
        register_tools(mcp, credentials=mock_creds)

        assert mock_creds.get("bigquery") == "/custom/path/credentials.json"
        assert mock_creds.get("bigquery_project") == "custom-project"

    def test_falls_back_to_env_vars(self, mcp):
        register_tools(mcp, credentials=None)

        expected_tools = {
            "run_bigquery_query",
            "describe_dataset",
            "bigquery_list_datasets",
            "bigquery_list_tables",
            "bigquery_get_table_info",
            "bigquery_preview_table",
            "bigquery_dry_run",
            "bigquery_list_jobs",
            "bigquery_get_job",
            "bigquery_list_projects",
        }
        assert expected_tools.issubset(set(mcp._tool_manager._tools))
