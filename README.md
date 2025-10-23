# Airflow DAGs Repository

This repository contains Apache Airflow DAG definitions for the Decepticon cluster.

## Repository Structure

```
airflow-dags/
├── README.md
└── dags/
    ├── configs/
    │   └── mongodb_to_questdb_etl.yaml
    └── mongodb_to_questdb_etl.py
```

## Deployment

This repository is automatically synced to Airflow via git-sync every 60 seconds.

Git-Sync Configuration:
- Branch: main
- Path: dags/
- Sync Interval: 60 seconds

## Configuration

Each DAG should have its own configuration file in `dags/configs/` following the naming pattern `<dag_name>.yaml`.

**Configuration files should contain**:
- Hostnames and ports
- Database/service names
- DAG-specific parameters (schedule, catchup, etc.)
- Non-sensitive settings

**Configuration files should NOT contain**:
- Passwords or API keys
- Connection strings with credentials
- Any sensitive data

**For secrets and credentials**, use:
- Airflow Connections (recommended)
- Environment variables
- External secret management (Vault, AWS Secrets Manager, etc.)

**Example**: `dags/configs/my_dag.yaml`
```yaml
# Database settings
database:
  host: "db.example.com"
  port: 5432
  name: "mydb"
  # Use Airflow Connection for credentials

# DAG settings
dag:
  schedule_interval: "0 * * * *"
  catchup: false
```

**Loading config in your DAG**:
```python
import yaml
from pathlib import Path
from airflow.hooks.base import BaseHook

# Load config
config_path = Path(__file__).parent / 'configs' / 'my_dag.yaml'
with open(config_path) as f:
    config = yaml.safe_load(f)

# Get connection details
db_host = config['database']['host']

# Get credentials from Airflow Connection
conn = BaseHook.get_connection('my_db_conn')
db_password = conn.password
```

## Development

### Adding a New DAG

1. Create a new Python file in the dags/ directory
2. Create a corresponding config file in dags/configs/ (e.g., `my_dag.yaml`)
3. Define your DAG using Airflow 3.x syntax
4. Load configuration from the config file in your DAG
5. Commit and push to main branch
6. DAG will appear in Airflow UI within 60 seconds

### Testing DAGs Locally

```bash
# Install Airflow
pip install apache-airflow==3.0.2

# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Copy DAGs to Airflow
cp dags/*.py $AIRFLOW_HOME/dags/

# Test DAG syntax
python dags/mongodb_to_questdb_etl.py

# Initialize Airflow DB
airflow db init

# Test DAG
airflow dags test mongodb_to_questdb_etl 2025-01-01
```

### DAG Best Practices

1. Always use catchup=False unless you need historical backfills
2. Set max_active_runs=1 for DAGs that shouldn't run concurrently
3. Use XCom to pass data between tasks (for small data)
4. Store configuration in per-DAG YAML files instead of hardcoding
5. Add proper logging for debugging
6. Handle errors gracefully with try/except blocks
7. Use execute_batch for bulk database operations
8. Tag your DAGs for organization
9. Keep DAG code and configuration together in this repository

## Troubleshooting

### DAG not appearing in UI

- Check that file is in dags/ directory
- Check for Python syntax errors: python dags/your_dag.py
- Check Airflow scheduler logs
- Wait up to 60 seconds for git-sync

### Task failures

- Check task logs in Airflow UI
- Verify database connectivity
- Check Airflow Variables are set correctly
- Verify service credentials

### Import errors

- Check that required dependencies are installed in Airflow image
- Contact cluster admin to add missing packages to the Docker image
