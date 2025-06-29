import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from typing import List, Dict, Tuple, Optional
from facebook_search_data_processor import FacebookSearchProcessor


class FacebookSearchProducer:
    """
    A Kafka producer that publishes processed Facebook data to Kafka topics.
    """
    
    def __init__(self, 
                 kafka_config: Dict,
                 topic: str = "facebook-search-data"):
        """
        Initialize the FacebookKafkaProducer.
        
        Args:
            kafka_config (dict): Kafka configuration (bootstrap_servers, etc.)
            topic (str): Kafka topic for Facebook search data
        """
        self.kafka_config = kafka_config
        self.topic = topic
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize Kafka producer
        self.producer = None
        self._init_kafka_producer()
        
        # Initialize counters
        self.published_posts = 0
        self.failed_posts = 0
        self.total_comments_published = 0
        
    def _init_kafka_producer(self):
        """Initialize Kafka producer with retry logic."""
        try:
            self.producer = KafkaProducer(
                **self.kafka_config,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                retries=3,
                retry_backoff_ms=1000,
                request_timeout_ms=30000,
                acks='all'  # Wait for all replicas to acknowledge
            )
            self.logger.info("✅ Kafka producer initialized successfully")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Kafka producer: {str(e)}")
            self.producer = None
            
    def _publish_to_kafka(self, topic: str, key: str, data: Dict) -> bool:
        """
        Publish data to Kafka topic.
        
        Args:
            topic (str): Kafka topic
            key (str): Message key
            data (dict): Data to publish
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.producer:
            self.logger.warning("⚠️ Kafka producer not available")
            return False
            
        try:
            future = self.producer.send(topic, key=key, value=data)
            
            # Wait for the message to be sent (with timeout)
            record_metadata = future.get(timeout=10)
            
            self.logger.info(f"✅ Published to {topic} - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            self.logger.error(f"❌ Kafka error publishing to {topic}: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Unexpected error publishing to {topic}: {str(e)}")
            return False
    
    def publish_post_with_comments(self, post: Dict) -> bool:
        """
        Publish a post with its comments to Kafka.
        
        Args:
            post (dict): Post data with comments
            
        Returns:
            bool: True if successful
        """
        post_key = f"{post.get('brand_name')}_{post.get('post_id')}"
        
        success = self._publish_to_kafka(self.topic, post_key, post)
        
        if success:
            self.published_posts += 1
            self.total_comments_published += len(post.get('comments', []))
            self.logger.info(f"✅ Published post {post.get('post_id')} with {len(post.get('comments', []))} comments")
        else:
            self.failed_posts += 1
            self.logger.error(f"❌ Failed to publish post {post.get('post_id')}")
            
        return success
    
    def publish_posts_batch(self, posts: List[Dict]) -> Dict:
        """
        Publish a batch of posts to Kafka.
        
        Args:
            posts (List[Dict]): List of processed posts
            
        Returns:
            dict: Publishing summary statistics
        """
        self.logger.info(f"🚀 Starting to publish {len(posts)} posts to Kafka topic: {self.topic}")
        
        # Reset counters
        self.published_posts = 0
        self.failed_posts = 0
        self.total_comments_published = 0
        
        try:
            for i, post in enumerate(posts, 1):
                self.logger.info(f"\n--- Publishing Post {i}/{len(posts)} ---")
                
                # Publish the post with comments
                self.publish_post_with_comments(post)
                
                # Add delay to avoid overwhelming Kafka (except for last post)
                if i < len(posts):
                    self.logger.info("⏳ Waiting 2 seconds before next post...")
                    time.sleep(2)
                    
        except Exception as e:
            self.logger.error(f"❌ Error during publishing: {str(e)}")
        
        finally:
            # Flush Kafka producer
            if self.producer:
                try:
                    self.producer.flush(timeout=30)
                    self.logger.info("✅ Kafka producer flushed successfully")
                except Exception as e:
                    self.logger.error(f"⚠️ Error flushing Kafka producer: {str(e)}")
        
        summary = self._create_publishing_summary()
        self._print_publishing_summary(summary)
        
        return summary
    
    def process_and_publish(self, 
                           processor: FacebookSearchProcessor) -> Tuple[Dict, Dict]:
        """
        Process Facebook data and publish to Kafka in one operation.
        
        Args:
            processor (FacebookSearchProcessor): Configured processor instance
            
        Returns:
            tuple: (processing_summary, publishing_summary)
        """
        # Process all posts
        processed_posts, processing_summary = processor.collect_and_process_all(self.topic)
        
        # Publish processed posts
        publishing_summary = self.publish_posts_batch(processed_posts)
        
        return processing_summary, publishing_summary
    
    def _create_publishing_summary(self) -> Dict:
        """Create publishing summary statistics."""
        return {
            'topic': self.topic,
            'publishing_time': datetime.now().isoformat(),
            'published_posts': self.published_posts,
            'total_comments_published': self.total_comments_published,
            'failed_posts': self.failed_posts,
            'total_published_messages': self.published_posts,  # Each post+comments = 1 Kafka message
            'success_rate': (self.published_posts / (self.published_posts + self.failed_posts) * 100) 
                           if (self.published_posts + self.failed_posts) > 0 else 0,
            'average_comments_per_post': (self.total_comments_published / self.published_posts) 
                                       if self.published_posts > 0 else 0
        }
    
    def _print_publishing_summary(self, summary: Dict):
        """Print publishing summary to console."""
        print("\n" + "="*60)
        print("🎉 FACEBOOK DATA KAFKA PUBLISHING COMPLETED")
        print("="*60)
        print(f"📡 Topic: {summary['topic']}")
        print(f"⏰ Published at: {summary['publishing_time']}")
        print("\n📊 PUBLISHING STATISTICS:")
        print(f"   ✅ Posts published: {summary['published_posts']}")
        print(f"   💬 Total comments published: {summary['total_comments_published']}")
        print(f"   📊 Average comments per post: {summary['average_comments_per_post']:.1f}")
        print(f"   ❌ Posts failed: {summary['failed_posts']}")
        print(f"   📈 Total Kafka messages: {summary['total_published_messages']}")
        print(f"   🎯 Success rate: {summary['success_rate']:.1f}%")
        print("="*60)
    
    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            try:
                self.producer.close(timeout=30)
                self.logger.info("✅ Kafka producer closed successfully")
            except Exception as e:
                self.logger.error(f"⚠️ Error closing Kafka producer: {str(e)}")


# Example usage
def main():
    """Example usage of FacebookKafkaProducer with FacebookSearchProcessor."""
    
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],  # Adjust as needed
        'client_id': 'facebook-search-producer',
        # Add other Kafka configs as needed (security, etc.)
    }
    
    # Initialize processor
    processor = FacebookSearchProcessor(
        apify_token="",
        brand_name="orangemaroc",
        search_query="#orangemaroc",
        max_posts=2,
        max_comments_per_post=2,
        post_time_range="30d"
    )
    
    # Initialize Kafka producer
    kafka_producer = FacebookProducer(
        kafka_config=kafka_config,
        topic="facebook-search-data-test"
    )
    
    try:
        # Method 1: Process and publish in one operation
        print("=== PROCESS AND PUBLISH ===")
        processing_summary, publishing_summary = kafka_producer.process_and_publish(processor)
        
        # Method 2: Process separately then publish
        # processed_posts, processing_summary = processor.collect_and_process_all("facebook-search-data-test")
        # publishing_summary = kafka_producer.publish_posts_batch(processed_posts)
        
    finally:
        # Always close the producer
        kafka_producer.close()


if __name__ == "__main__":
    main()