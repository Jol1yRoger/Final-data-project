
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

import os
import sys

dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from utils.kafka_utils import create_kafka_consumer, close_consumer
from utils.db_utils import init_database, insert_events
from utils.cleaning_rules import clean_game_event
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_EVENTS,
    KAFKA_CONSUMER_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    DB_PATH
)

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def ensure_database_initialized(**context):
    logger.info("Initializing database if needed")
    success = init_database(DB_PATH)
    if not success:
        raise Exception("Failed to initialize database")
    logger.info("Database initialization complete")


def clean_and_store_events(**context):
    logger.info("Starting hourly batch cleaning and storage job")
    
    consumer = create_kafka_consumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC_RAW_EVENTS,
        group_id=KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET
    )
    
    if not consumer:
        logger.error("Failed to create Kafka consumer, aborting")
        raise Exception("Kafka consumer creation failed")
    
    cleaned_events = []
    raw_count = 0
    
    try:
        logger.info("Reading messages from Kafka")
        
        for message in consumer:
            raw_count += 1
            raw_event = message.value
            
            cleaned_event = clean_game_event(raw_event)
            
            if cleaned_event:
                cleaned_events.append(cleaned_event)
            
            if raw_count % 100 == 0:
                logger.info(f"Processed {raw_count} raw events, {len(cleaned_events)} cleaned")
        
        logger.info(f"Finished reading from Kafka. Raw: {raw_count}, Cleaned: {len(cleaned_events)}")
        
        if cleaned_events:
            inserted = insert_events(DB_PATH, cleaned_events)
            logger.info(f"Inserted {inserted} events into database")
            
            consumer.commit()
            logger.info("Kafka offsets committed")
        else:
            logger.info("No events to insert")
    
    except Exception as e:
        logger.error(f"Error during cleaning and storage: {e}")
        raise
    
    finally:
        close_consumer(consumer)
        logger.info("Batch cleaning and storage completed")


with DAG(
    'dag2_hourly_cleaning',
    default_args=default_args,
    description='Hourly batch job to clean Kafka data and store in SQLite',
    schedule_interval='@hourly',
    catchup=False,
    tags=['lichess', 'cleaning', 'sqlite'],
) as dag:
    
    init_db_task = PythonOperator(
        task_id='initialize_database',
        python_callable=ensure_database_initialized,
    )
    
    clean_store_task = PythonOperator(
        task_id='clean_and_store_events',
        python_callable=clean_and_store_events,
        execution_timeout=timedelta(minutes=30),
    )
    
    init_db_task >> clean_store_task
