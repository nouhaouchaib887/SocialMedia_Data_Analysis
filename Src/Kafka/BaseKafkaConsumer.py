from confluent_kafka import Consumer, KafkaError
import json
import os
from datetime import datetime
import time
import logging
import pymongo

class BaseKafkaConsumer:
    """
    Base consumer class that handles common functionality for consuming social media data:
    - Consumes messages from Kafka
    - Handles message processing workflow
    - Stores results in both JSON files and MongoDB
    
    To be extended by platform-specific consumers like TrustpilotConsumer, FacebookConsumer, etc.
    """
    
    def __init__(self, 
                 kafka_config, 
                 topics, 
                 output_dir="data/processed", 
                 mongodb_uri="mongodb://localhost:27017/", 
                 db_name="social_media_db",
                 collection_name="reviews",
                 consumer_name="BaseConsumer"):
        """
        Initialize the BaseConsumer.
        
        Args:
            kafka_config (dict): Kafka consumer configuration
            topics (list): List of Kafka topics to consume from
            output_dir (str): Directory to save JSON output files
            mongodb_uri (str): MongoDB connection URI
            db_name (str): MongoDB database name
            collection_name (str): MongoDB collection name
            consumer_name (str): Name for the consumer (used for logging)
        """
        # Set up consumer configurations
        self.kafka_config = kafka_config.copy()
        self.kafka_config.update({
            'group.id': kafka_config.get('group.id', f'{consumer_name.lower()}_group'),
            'auto.offset.reset': kafka_config.get('auto.offset.reset', 'earliest'),
            'enable.auto.commit': kafka_config.get('enable.auto.commit', True),
        })
        
        self.topics = topics if isinstance(topics, list) else [topics]
        self.output_dir = output_dir
        self.consumer_name = consumer_name
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Setup logging
        self.logger = logging.getLogger(self.consumer_name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            # Add file handler
            file_handler = logging.FileHandler(os.path.join(self.output_dir, f'{consumer_name.lower()}.log'))
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # Initialize Kafka consumer
        self.consumer = Consumer(self.kafka_config)
        self.consumer.subscribe(self.topics)
        
        # Setup MongoDB connection
        try:
            self.mongo_client = pymongo.MongoClient(mongodb_uri)
            self.db = self.mongo_client[db_name]
            self.collection = self.db[collection_name]
            self.logger.info(f"Connected to MongoDB: {db_name}.{collection_name}")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            self.mongo_client = None
            self.db = None
            self.collection = None
            
        self.logger.info(f"Initialized {self.consumer_name} for topics: {self.topics}")
    
    def process_message(self, message):
        """
        Process a single Kafka message. This is a template method to be overridden by subclasses.
        
        Args:
            message: Kafka message object
            
        Returns:
            list: Processed and enriched data items
        """
        if message is None:
            return None
            
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                self.logger.info(f"Reached end of partition for topic {message.topic()}")
            else:
                self.logger.error(f"Error consuming message: {message.error()}")
            return None
            
        # Parse message value
        try:
            # Decode the message value from bytes to string
            value_str = message.value().decode('utf-8')
            data = json.loads(value_str)
            
            # This method should be implemented by platform-specific subclasses
            return self._process_data(data)
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding message: {e}")
            self.logger.error(f"Raw message: {message.value()}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return None
    
    def _process_data(self, data):
        """
        Abstract method to be implemented by subclasses for platform-specific data processing.
        
        Args:
            data (dict): Parsed JSON data from Kafka message
            
        Returns:
            list: List of processed items
        """
        raise NotImplementedError("Subclasses must implement _process_data method")
    
    def enrich_data(self, item):
        """
        Abstract method to be implemented by subclasses for data enrichment.
        
        Args:
            item: Data item to be enriched
            
        Returns:
            dict: Enriched data item
        """
        raise NotImplementedError("Subclasses must implement enrich_data method")
    
    
    
    def save_to_mongodb(self, data):
        """
        Save enriched data to MongoDB.
        
        Args:
            data (list): List of enriched items
            
        Returns:
            int: Number of documents inserted
        """
        if not data or not self.collection:
            return 0
            
        try:
            # Insert data into MongoDB
            result = self.collection.insert_many(data)
            inserted_count = len(result.inserted_ids)
            self.logger.info(f"Inserted {inserted_count} documents into MongoDB")
            return inserted_count
        except Exception as e:
            self.logger.error(f"Error saving to MongoDB: {e}")
            return 0
    
    def consume_batch(self, timeout=1.0, max_messages=100, commit=True):
        """
        Consume a batch of messages from Kafka.
        
        Args:
            timeout (float): Maximum time to wait for messages in seconds
            max_messages (int): Maximum number of messages to consume in one batch
            commit (bool): Whether to commit offsets after processing
            
        Returns:
            list: List of processed items
        """
        messages = []
        start_time = time.time()
        
        # Collect messages
        while len(messages) < max_messages and (time.time() - start_time) < timeout:
            msg = self.consumer.poll(timeout=0.1)
            if msg:
                messages.append(msg)
        
        self.logger.info(f"Consumed {len(messages)} messages from Kafka")
        
        # Process messages
        all_processed_items = []
        for msg in messages:
            processed_items = self.process_message(msg)
            if processed_items:
                all_processed_items.extend(processed_items)
        
        # Commit offsets if requested
        if commit and messages:
            self.consumer.commit()
            
        self.logger.info(f"Processed {len(all_processed_items)} items")
        return all_processed_items
    
    def run(self, processing_interval=5.0, batch_size=100, save_interval=50):
        """
        Run the consumer in a continuous loop.
        
        Args:
            processing_interval (float): Seconds between each batch processing
            batch_size (int): Maximum number of messages to process in each batch
            save_interval (int): Number of items to accumulate before saving
            
        This will run until interrupted with Ctrl+C
        """
        self.logger.info(f"Starting {self.consumer_name}...")
        all_items = []
        current_platform = None
        
        try:
            while True:
                # Consume and process a batch of messages
                processed_items = self.consume_batch(
                    timeout=processing_interval,
                    max_messages=batch_size
                )
                
                # Skip if no new items
                if not processed_items:
                    time.sleep(processing_interval)
                    continue
                    
                # Detect platform from items (if applicable)
                if processed_items and 'platform' in processed_items[0]:
                    current_platform = processed_items[0]['platform']
                
                # Add to accumulated items
                all_items.extend(processed_items)
                
                # Save when we reach the threshold
                if len(all_items) >= save_interval:
                    
                    # Save to MongoDB
                    if self.collection is not None:
                        self.save_to_mongodb(all_items)
                        
                    # Clear accumulated items
                    all_items = []
                    
        except KeyboardInterrupt:
            self.logger.info(f"{self.consumer_name} interrupted by user")
        except Exception as e:
            self.logger.error(f"Error in consumer loop: {e}")
        finally:
            # Save any remaining items
            if all_items:
                if self.collection is not None:
                    self.save_to_mongodb(all_items)
                    
            # Close consumer
            self.consumer.close()
            if self.mongo_client:
                self.mongo_client.close()
                
            self.logger.info(f"{self.consumer_name} stopped")