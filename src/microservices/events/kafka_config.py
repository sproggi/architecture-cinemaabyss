import json
import logging
from typing import Any, Dict, Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaConfig:
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        
        # Topic configurations
        self.topics = {
            "movie": "movie-events",
            "user": "user-events", 
            "payment": "payment-events"
        }
    
    async def init_producer(self, retries=5, delay=2):
        """Initialize Kafka producer with retries"""
        for attempt in range(retries):
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                await self.producer.start()
                logger.info("Kafka producer started successfully")
                return True
            except Exception as e:
                logger.warning(f"Producer init attempt {attempt + 1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error("Failed to start Kafka producer after all retries")
        return False
    
    async def init_consumer(self, topics: list, retries=5, delay=2):
        """Initialize Kafka consumer with retries"""
        for attempt in range(retries):
            try:
                self.consumer = AIOKafkaConsumer(
                    *topics,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id="events-service-group",
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset="earliest",
                    enable_auto_commit=True
                )
                await self.consumer.start()
                logger.info(f"Kafka consumer started for topics: {topics}")
                return True
            except Exception as e:
                logger.warning(f"Consumer init attempt {attempt + 1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error("Failed to start Kafka consumer after all retries")
        return False
    
    async def produce_event(self, topic: str, event_data: Dict[str, Any]) -> bool:
        """Produce an event to Kafka with retries"""
        if not self.producer:
            logger.error("Producer not initialized")
            return False
        
        try:
            await self.producer.send(topic, event_data)
            logger.info(f"Event produced to topic '{topic}': {event_data}")
            return True
        except Exception as e:
            logger.error(f"Failed to produce event: {e}")
            return False
    
    async def consume_events(self, topics: list):
        """Consume events from Kafka topics"""
        if not self.consumer:
            logger.error("Consumer not initialized")
            return
        
        try:
            async for msg in self.consumer:
                logger.info(f"Consumed event from topic '{msg.topic}': {msg.value}")
                # Log the event with details
                self._log_event(msg.topic, msg.value)
        except Exception as e:
            logger.error(f"Error consuming events: {e}")
    
    def _log_event(self, topic: str, data: Dict[str, Any]):
        """Log event with proper formatting"""
        event_type = topic.split('-')[0] if '-' in topic else 'unknown'
        event_id = data.get('event_id', 'unknown')
        event_data = data.get('data', {})
        
        logger.info(f"📨 Event Type: {event_type.upper()}")
        logger.info(f"   Event ID: {event_id}")
        logger.info(f"   Data: {json.dumps(event_data, indent=2)}")
        logger.info("   " + "-" * 50)
    
    async def close(self):
        """Close producer and consumer connections"""
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()


# Singleton instance
kafka_config = KafkaConfig()