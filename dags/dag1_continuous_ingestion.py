
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import time
import logging

import os
import sys

dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from utils.kafka_utils import create_kafka_producer, send_to_kafka, close_producer
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_EVENTS,
    LICHESS_API_BASE_URL,
    LICHESS_TV_FEED_ENDPOINT,
    API_FETCH_INTERVAL_SECONDS
)

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def ingest_lichess_data(**context):

    logger.info("Starting continuous data ingestion from Lichess API")
    
    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    if not producer:
        logger.error("Failed to create Kafka producer, aborting")
        raise Exception("Kafka producer creation failed")
    
    api_url = f"{LICHESS_API_BASE_URL}{LICHESS_TV_FEED_ENDPOINT}"
    logger.info(f"Connecting to Lichess API: {api_url}")
    
    start_time = time.time()
    max_runtime = 3600  
    message_count = 0
    
    try:
        with requests.get(api_url, stream=True, timeout=30) as response:
            response.raise_for_status()
            logger.info("Connected to Lichess TV feed stream")
            
            for line in response.iter_lines():
                if time.time() - start_time > max_runtime:
                    logger.info(f"Max runtime reached, stopping ingestion. Processed {message_count} messages")
                    break
                
                if line:
                    try:
                        event = json.loads(line.decode('utf-8'))
                        
                        game_id = event.get('d', {}).get('id', 'unknown')
                        success = send_to_kafka(
                            producer=producer,
                            topic=KAFKA_TOPIC_RAW_EVENTS,
                            message=event,
                            key=game_id
                        )
                        
                        if success:
                            message_count += 1
                            if message_count % 10 == 0:
                                logger.info(f"Sent {message_count} messages to Kafka")
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON line: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
                        continue
                
                time.sleep(0.1)
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise
    
    finally:
        close_producer(producer)
        logger.info(f"Ingestion completed. Total messages sent: {message_count}")


with DAG(
    'dag1_continuous_ingestion',
    default_args=default_args,
    description='Continuous ingestion of Lichess game data to Kafka',
    schedule_interval=None,
    catchup=False,
    tags=['lichess', 'ingestion', 'kafka'],
) as dag:
    
    ingest_task = PythonOperator(
        task_id='ingest_lichess_to_kafka',
        python_callable=ingest_lichess_data,
        execution_timeout=timedelta(hours=2),
    )
    
    ingest_task
