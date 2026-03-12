# BigQuery Tool

Execute read-only SQL queries and explore datasets in Google BigQuery.

## Features

- `run_bigquery_query`: Execute read-only SQL queries and return structured results
- `describe_dataset`: List tables and schemas in a dataset for query planning
- `bigquery_list_datasets`: List datasets available in a project
- `bigquery_list_tables`: List tables in a dataset without fetching full schemas
- `bigquery_get_table_info`: Get schema and metadata for a single table
- `bigquery_preview_table`: Preview the first rows of a table without writing SQL
- `bigquery_dry_run`: Validate a read-only query and estimate bytes processed
- `bigquery_list_jobs`: List recent BigQuery jobs
- `bigquery_get_job`: Get detailed status for a specific BigQuery job
- `bigquery_list_projects`: List accessible Google Cloud projects

## Setup

### 1. Install Dependencies

The BigQuery tool requires `google-cloud-bigquery`:

```bash
pip install google-cloud-bigquery>=3.0.0
```

`bigquery_list_projects` also uses Google Cloud Resource Manager when available:

```bash
pip install google-cloud-resource-manager
```

This extra dependency is optional. If it is not installed, only `bigquery_list_projects`
returns an error with install guidance.

### 2. Configure Authentication

Choose one of the following authentication methods:

#### Option A: Service Account (Recommended for Production)

1. Create a service account in Google Cloud Console
2. Grant the following roles:
   - `BigQuery Data Viewer`
   - `BigQuery Job User`
3. Download the JSON key file
4. Set the environment variable:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

#### Option B: Application Default Credentials (For Local Development)

```bash
gcloud auth application-default login
```

### 3. Set Default Project (Optional)

If your queries do not specify a project, set a default:

```bash
export BIGQUERY_PROJECT_ID="your-project-id"
```

## Recommended Workflow

The tool now supports a full:

`discover -> preview -> estimate -> execute -> monitor`

workflow.

### 1. Discover projects and datasets

```python
bigquery_list_projects()
bigquery_list_datasets(project_id="my-project")
```

### 2. Discover tables and inspect metadata

```python
bigquery_list_tables(dataset_id="analytics", project_id="my-project")
bigquery_get_table_info(dataset_id="analytics", table_id="users", project_id="my-project")
```

### 3. Preview data

```python
bigquery_preview_table(
    dataset_id="analytics",
    table_id="users",
    project_id="my-project",
    max_rows=10,
)
```

### 4. Estimate cost before execution

```python
bigquery_dry_run(
    sql="SELECT id, email FROM `my-project.analytics.users` WHERE active = TRUE",
    project_id="my-project",
)
```

### 5. Execute the query

```python
run_bigquery_query(
    sql="SELECT id, email FROM `my-project.analytics.users` WHERE active = TRUE",
    project_id="my-project",
    max_rows=100,
)
```

### 6. Monitor jobs

```python
bigquery_list_jobs(project_id="my-project", max_results=10)
bigquery_get_job(job_id="job_123", project_id="my-project")
```

## Safety Features

### Read-Only Enforcement

The tool blocks write operations for safety. The following SQL keywords are rejected:

- `INSERT`
- `UPDATE`
- `DELETE`
- `DROP`
- `CREATE`
- `ALTER`
- `TRUNCATE`
- `MERGE`
- `REPLACE`

### Limits

- `run_bigquery_query` defaults to 1000 rows and caps at 10,000
- `bigquery_preview_table` defaults to 20 rows and caps at 1,000
- `bigquery_list_jobs` defaults to 20 jobs and caps at 100

### Cost Awareness

- `run_bigquery_query` returns `bytes_processed`
- `bigquery_dry_run` returns `estimated_bytes_processed`

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | No* | Path to service account JSON file |
| `BIGQUERY_PROJECT_ID` | No | Default project ID for queries |

*Required if not using Application Default Credentials (ADC)

## Error Handling

The tool returns structured error responses with helpful messages:

```python
{
    "error": "BigQuery authentication failed",
    "help": "Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path, or run 'gcloud auth application-default login' for local development."
}
```

```python
{
    "error": "Write operations are not allowed",
    "help": "Only SELECT queries are permitted. INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, and MERGE are blocked."
}
```

```python
{
    "error": "google-cloud-resource-manager is required for bigquery_list_projects. Install it with: pip install google-cloud-resource-manager",
    "help": "Install the optional dependency by running: pip install google-cloud-resource-manager"
}
```

## Troubleshooting

### "Could not automatically determine credentials"

- Set `GOOGLE_APPLICATION_CREDENTIALS`, or
- Run `gcloud auth application-default login`

### "Permission denied"

Ensure your service account has:

- `roles/bigquery.dataViewer`
- `roles/bigquery.jobUser`

### "Dataset not found" or "Table not found"

- Check the dataset and table names
- Verify the project ID
- Ensure you have access to the resource
