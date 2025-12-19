
import json
import logging
from typing import Optional, Dict, Any
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


def create_kafka_producer(bootstrap_servers: str) -> Optional[KafkaProducer]:

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"Kafka producer created successfully: {bootstrap_servers}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def create_kafka_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str = 'earliest'
) -> Optional[KafkaConsumer]:
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,  # Manual commit for better control
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000  # 10 second timeout
        )
        logger.info(f"Kafka consumer created for topic '{topic}', group '{group_id}'")
        return consumer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None


def send_to_kafka(
    producer: KafkaProducer,
    topic: str,
    message: Dict[Any, Any],
    key: Optional[str] = None
) -> bool:
    try:
        future = producer.send(topic, value=message, key=key)
        record_metadata = future.get(timeout=10)
        logger.debug(
            f"Message sent to {record_metadata.topic} "
            f"partition {record_metadata.partition} "
            f"offset {record_metadata.offset}"
        )
        return True
    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending to Kafka: {e}")
        return False


def close_producer(producer: KafkaProducer) -> None:
    try:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed successfully")
    except Exception as e:
        logger.error(f"Error closing Kafka producer: {e}")


def close_consumer(consumer: KafkaConsumer) -> None:
    try:
        consumer.close()
        logger.info("Kafka consumer closed successfully")
    except Exception as e:
        logger.error(f"Error closing Kafka consumer: {e}")
