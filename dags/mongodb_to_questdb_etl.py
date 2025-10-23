"""
MongoDB to QuestDB ETL Pipeline
Extracts data from MongoDB and loads into QuestDB time-series database
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

# Import required libraries (need to be installed in Airflow environment)
try:
    import pymongo
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError as e:
    logging.error(f"Missing required library: {e}")
    logging.error("Install with: pip install pymongo psycopg2-binary")

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'mongodb_to_questdb_etl',
    default_args=default_args,
    description='ETL pipeline from MongoDB to QuestDB',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs
    tags=['etl', 'mongodb', 'questdb'],
)

# Configuration (can be overridden with Airflow Variables)
MONGO_URI = Variable.get("MONGO_URI", default_var="mongodb://mongodb.default.svc.cluster.local:27017/")
MONGO_DATABASE = Variable.get("MONGO_DATABASE", default_var="mydb")
MONGO_COLLECTION = Variable.get("MONGO_COLLECTION", default_var="readings")

QUESTDB_HOST = Variable.get("QUESTDB_HOST", default_var="questdb.questdb.svc.cluster.local")
QUESTDB_PORT = Variable.get("QUESTDB_PORT", default_var="8812")
QUESTDB_USER = Variable.get("QUESTDB_USER", default_var="admin")
QUESTDB_PASSWORD = Variable.get("QUESTDB_PASSWORD", default_var="quest")
QUESTDB_DATABASE = Variable.get("QUESTDB_DATABASE", default_var="qdb")

# Batch size for bulk inserts
BATCH_SIZE = 1000


def get_last_sync_timestamp(**context):
    """
    Get the timestamp of the last successful sync.
    Stores in XCom for next task.
    """
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database=QUESTDB_DATABASE
        )
        cursor = conn.cursor()

        # Get the latest timestamp from QuestDB
        # Adjust table name and column as needed
        cursor.execute("""
            SELECT MAX(timestamp)
            FROM readings
        """)

        result = cursor.fetchone()
        last_sync = result[0] if result[0] else datetime(2020, 1, 1)

        cursor.close()
        conn.close()

        logging.info(f"Last sync timestamp: {last_sync}")
        return last_sync

    except psycopg2.Error as e:
        logging.warning(f"Could not get last sync timestamp (table may not exist): {e}")
        # Return a default start date if table doesn't exist yet
        return datetime(2020, 1, 1)


def extract_from_mongodb(**context):
    """
    Extract data from MongoDB since last sync.
    Returns list of documents.
    """
    ti = context['task_instance']
    last_sync = ti.xcom_pull(task_ids='get_last_sync_timestamp')

    # Convert to datetime if needed
    if isinstance(last_sync, str):
        last_sync = datetime.fromisoformat(last_sync)

    logging.info(f"Extracting MongoDB data since: {last_sync}")

    # Connect to MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    # Query for new documents
    # Adjust query based on your data structure
    query = {
        "timestamp": {"$gt": last_sync}
    }

    # Extract data
    documents = list(collection.find(query).sort("timestamp", 1))

    logging.info(f"Extracted {len(documents)} documents from MongoDB")

    client.close()

    # Push to XCom for next task
    return documents


def transform_data(**context):
    """
    Transform MongoDB documents to QuestDB format.
    Adjust this function based on your data structure.
    """
    ti = context['task_instance']
    documents = ti.xcom_pull(task_ids='extract_from_mongodb')

    if not documents:
        logging.info("No documents to transform")
        return []

    logging.info(f"Transforming {len(documents)} documents")

    transformed = []

    for doc in documents:
        try:
            # Example transformation for CGM data
            # Adjust based on your actual MongoDB schema
            transformed_record = {
                'timestamp': doc.get('timestamp'),
                'user_id': doc.get('user_id'),
                'glucose_mg_dl': doc.get('glucose', {}).get('value'),
                'trend_arrow': doc.get('glucose', {}).get('trend'),
                'device_id': doc.get('device_id'),
            }

            # Data validation
            if all([
                transformed_record['timestamp'],
                transformed_record['glucose_mg_dl'] is not None
            ]):
                transformed.append(transformed_record)
            else:
                logging.warning(f"Skipping invalid record: {doc.get('_id')}")

        except Exception as e:
            logging.error(f"Error transforming document {doc.get('_id')}: {e}")
            continue

    logging.info(f"Successfully transformed {len(transformed)} records")

    return transformed


def load_to_questdb(**context):
    """
    Load transformed data into QuestDB via PostgreSQL wire protocol.
    Uses batch inserts for performance.
    """
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='transform_data')

    if not data:
        logging.info("No data to load into QuestDB")
        return 0

    logging.info(f"Loading {len(data)} records into QuestDB")

    # Connect to QuestDB
    conn = psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DATABASE
    )

    cursor = conn.cursor()

    try:
        # Create table if it doesn't exist
        # Adjust schema based on your data structure
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS readings (
                timestamp TIMESTAMP,
                user_id SYMBOL,
                glucose_mg_dl INT,
                trend_arrow SYMBOL,
                device_id SYMBOL
            ) timestamp(timestamp) PARTITION BY DAY;
        """)

        # Prepare batch insert
        insert_query = """
            INSERT INTO readings (
                timestamp,
                user_id,
                glucose_mg_dl,
                trend_arrow,
                device_id
            ) VALUES (%s, %s, %s, %s, %s)
        """

        # Convert data to tuples
        values = [
            (
                record['timestamp'],
                record['user_id'],
                record['glucose_mg_dl'],
                record['trend_arrow'],
                record['device_id']
            )
            for record in data
        ]

        # Batch insert for performance
        execute_batch(cursor, insert_query, values, page_size=BATCH_SIZE)

        conn.commit()

        logging.info(f"Successfully loaded {len(data)} records into QuestDB")

        return len(data)

    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading data to QuestDB: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def send_completion_notification(**context):
    """
    Send notification about ETL completion.
    Optional: integrate with Slack, email, etc.
    """
    ti = context['task_instance']
    records_loaded = ti.xcom_pull(task_ids='load_to_questdb')

    logging.info(f"ETL completed successfully. Loaded {records_loaded} records.")

    # Optional: Send to Slack, email, etc.
    # Example:
    # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    # SlackWebhookOperator(
    #     task_id='slack_notification',
    #     http_conn_id='slack_webhook',
    #     message=f'MongoDB → QuestDB ETL completed: {records_loaded} records',
    #     dag=dag
    # )


# Define tasks
get_last_sync = PythonOperator(
    task_id='get_last_sync_timestamp',
    python_callable=get_last_sync_timestamp,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_questdb',
    python_callable=load_to_questdb,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    trigger_rule='all_success',
    dag=dag,
)

# Define task dependencies
get_last_sync >> extract_task >> transform_task >> load_task >> notify_task
