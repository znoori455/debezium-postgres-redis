#!/usr/bin/env python3
"""
CDC Consumer: Processes Debezium events and updates Redis
Implements exactly-once semantics using Kafka offsets stored in Redis
"""

import json
import os
import time
import logging
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata
import redis
import debugpy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
TOPIC_NAME = 'dbserver1.public.users'
CONSUMER_GROUP = 'cdc-redis-consumer'

# Redis key prefixes
USER_KEY_PREFIX = 'user:'
OFFSET_KEY_PREFIX = 'kafka_offset:'


class CDCConsumer:
    """
    CDC Consumer that ensures exactly-once processing by:
    1. Using Kafka consumer groups for at-least-once delivery
    2. Storing offsets in Redis alongside data updates (transactional-like)
    3. Checking processed offsets before applying changes
    """
    
    def __init__(self):
        self.redis_client = None
        self.consumer = None
        self.setup_connections()
    
    def setup_connections(self):
        """Initialize Redis and Kafka connections with retries"""
        # Connect to Redis
        max_retries = 30
        for i in range(max_retries):
            try:
                self.redis_client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                self.redis_client.ping()
                logger.info("✓ Connected to Redis")
                break
            except Exception as e:
                logger.warning(f"Redis connection attempt {i+1}/{max_retries} failed: {e}")
                time.sleep(2)
        
        # Wait for Kafka topic to be created
        time.sleep(5)
        
        # Connect to Kafka
        for i in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=CONSUMER_GROUP,
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,  # Manual commit for exactly-once
                    value_deserializer=lambda m: {} if m is None else json.loads(m.decode('utf-8')),
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                self.consumer.subscribe([TOPIC_NAME])
                logger.info(f"✓ Connected to Kafka, subscribed to {TOPIC_NAME}")
                break
            except Exception as e:
                logger.warning(f"Kafka connection attempt {i+1}/{max_retries} failed: {e}")
                time.sleep(2)
    
    def get_offset_key(self, partition: int) -> str:
        """Generate Redis key for storing Kafka offset"""
        return f"{OFFSET_KEY_PREFIX}{TOPIC_NAME}:{partition}"
    
    def has_processed_offset(self, partition: int, offset: int) -> bool:
        """Check if this offset has already been processed"""
        key = self.get_offset_key(partition)
        last_offset = self.redis_client.get(key)
        if last_offset is None:
            return False
        return int(last_offset) >= offset
    
    def process_event(self, event: Dict[str, Any], partition: int, offset: int):
        """
        Process a single CDC event and update Redis.
        Uses a pipeline to ensure atomicity of data update + offset storage.
        """
        
        # Check if already processed (idempotency)
        if self.has_processed_offset(partition, offset):
            logger.info(f"Skipping already processed offset {offset} on partition {partition}")
            return
        
        payload = event or {}
        operation = payload.get('op')
        
        if not operation:
            logger.warning(f"No operation found in event: {event}")
            return
        
        logger.info(f"Processing {operation} operation (partition={partition}, offset={offset})")
        
        # Use Redis pipeline for atomic operations
        pipe = self.redis_client.pipeline()
        
        try:
            if operation in ['c', 'r', 'u']:  # Create, Read (snapshot), Update
                after = payload.get('after', {})
                if after and 'id' in after:
                    user_id = after['id']
                    user_key = f"{USER_KEY_PREFIX}{user_id}"
                    
                    # Store user data as hash
                    user_data = {
                        'id': str(after.get('id', '')),
                        'name': after.get('name', ''),
                        'email': after.get('email', ''),
                        'created_at': str(after.get('created_at', '')),
                        'updated_at': str(after.get('updated_at', ''))
                    }
                    
                    pipe.hset(user_key, mapping=user_data)
                    logger.info(f"  → Stored user {user_id}: {user_data['name']} ({user_data['email']})")
            
            elif operation == 'd':  # Delete
                before = payload.get('before', {})
                if before and 'id' in before:
                    user_id = before['id']
                    user_key = f"{USER_KEY_PREFIX}{user_id}"
                    pipe.delete(user_key)
                    logger.info(f"  → Deleted user {user_id}")
            
            # Store the offset (critical for exactly-once)
            offset_key = self.get_offset_key(partition)
            pipe.set(offset_key, offset)
            
            # Execute pipeline atomically
            pipe.execute()
            logger.info(f"✓ Event processed successfully (offset {offset} stored)")
            
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            raise
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting CDC consumer...")
        logger.info("Waiting for Debezium connector to be configured...")
        
        try:
            while True:
                # Poll for messages
                msg_batch = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                if not msg_batch:
                    print('no messages')
                    continue
                
                # Process messages
                for tp, messages in msg_batch.items():
                    for message in messages:
                        try:
                            self.process_event(
                                message.value,
                                message.partition,
                                message.offset
                            )
                            
                            # Commit offset to Kafka after successful processing
                            # This provides at-least-once delivery
                            # Combined with our idempotency check, we get exactly-once
                            self.consumer.commit({tp: OffsetAndMetadata(offset=message.offset, metadata='')})
                            
                        except Exception as e:
                            logger.error(f"Failed to process message: {e}", exc_info=True)
                            # Don't commit on error - message will be reprocessed
                            time.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Consumer stopped")


if __name__ == '__main__':
    debugpy.listen(("0.0.0.0", 5678))
    debugpy.wait_for_client()
    logger.info('bye!')
    consumer = CDCConsumer()
    consumer.run()