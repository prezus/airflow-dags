Here's the README for your airflow-dags repository:

  # Airflow DAGs Repository

  This repository contains Apache Airflow DAG definitions for the Decepticon cluster.

  ## Repository Structure

  airflow-dags/
  ├── README.md
  └── dags/
      ├── mongodb_to_questdb_etl.py
      └── (other DAG files)

  ## DAGs

  ### mongodb_to_questdb_etl.py

  **Purpose**: ETL pipeline to sync data from MongoDB to QuestDB time-series database

  **Schedule**: Every 15 minutes (`*/15 * * * *`)

  **Tasks**:
  1. `get_last_sync_timestamp` - Retrieves the latest timestamp from QuestDB
  2. `extract_from_mongodb` - Extracts new documents from MongoDB since last sync
  3. `transform_data` - Transforms MongoDB documents to QuestDB schema
  4. `load_to_questdb` - Bulk loads data into QuestDB
  5. `send_notification` - Logs completion status

  **Configuration Variables** (set in Airflow UI):
  - `MONGO_URI` - MongoDB connection string (default: `mongodb://mongodb.default.svc.cluster.local:27017/`)
  - `MONGO_DATABASE` - MongoDB database name (default: `mydb`)
  - `MONGO_COLLECTION` - MongoDB collection name (default: `readings`)
  - `QUESTDB_HOST` - QuestDB hostname (default: `questdb.questdb.svc.cluster.local`)
  - `QUESTDB_PORT` - QuestDB PostgreSQL port (default: `8812`)
  - `QUESTDB_USER` - QuestDB username (default: `admin`)
  - `QUESTDB_PASSWORD` - QuestDB password (default: `quest`)
  - `QUESTDB_DATABASE` - QuestDB database (default: `qdb`)

  **Dependencies**:
  ```python
  pymongo
  psycopg2-binary

  Deployment

  This repository is automatically synced to Airflow via git-sync every 60 seconds.

  Airflow Instance: https://airflow.decepticon.io

  Git-Sync Configuration:
  - Branch: main
  - Path: dags/
  - Sync Interval: 60 seconds

  Development

  Adding a New DAG

  1. Create a new Python file in the dags/ directory
  2. Define your DAG using Airflow 3.x syntax
  3. Commit and push to main branch
  4. DAG will appear in Airflow UI within 60 seconds

  Testing DAGs Locally

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

  DAG Best Practices

  1. Always use catchup=False unless you need historical backfills
  2. Set max_active_runs=1 for DAGs that shouldn't run concurrently
  3. Use XCom to pass data between tasks (for small data)
  4. Use Variables for configuration instead of hardcoding
  5. Add proper logging for debugging
  6. Handle errors gracefully with try/except blocks
  7. Use execute_batch for bulk database operations
  8. Tag your DAGs for organization

  Customizing the MongoDB → QuestDB ETL

  Adjust MongoDB Query

  Edit the extract_from_mongodb function:

  query = {
      "timestamp": {"$gt": last_sync},
      "status": "active"  # Add filters
  }

  Modify Data Transformation

  Edit the transform_data function to match your schema:

  transformed_record = {
      'timestamp': doc.get('timestamp'),
      'your_field': doc.get('your_field'),
      # Add your fields here
  }

  Update QuestDB Schema

  Edit the load_to_questdb function's CREATE TABLE statement:

  CREATE TABLE IF NOT EXISTS your_table (
      timestamp TIMESTAMP,
      your_field SYMBOL,
      -- Add your columns
  ) timestamp(timestamp) PARTITION BY DAY;

  Setting Airflow Variables

  In Airflow UI (https://airflow.decepticon.io):

  1. Go to Admin → Variables
  2. Click + to add new variable
  3. Set Key/Value pairs (see Configuration Variables above)

  Troubleshooting

  DAG not appearing in UI

  - Check that file is in dags/ directory
  - Check for Python syntax errors: python dags/your_dag.py
  - Check Airflow scheduler logs
  - Wait up to 60 seconds for git-sync

  Task failures

  - Check task logs in Airflow UI
  - Verify database connectivity
  - Check Airflow Variables are set correctly
  - Verify MongoDB/QuestDB credentials

  Import errors

  Dependencies need to be installed in Airflow image. Contact cluster admin to add:
  RUN pip install pymongo psycopg2-binary

  Support

  - Airflow UI: https://airflow.decepticon.io
  - Documentation: https://airflow.apache.org/docs/
  - Cluster Issues: Contact DevOps team

  License

  Internal use only - Decepticon cluster

  This README provides:
  - Repository structure
  - DAG documentation
  - Configuration instructions
  - Development guidelines
  - Customization examples
  - Troubleshooting tips
