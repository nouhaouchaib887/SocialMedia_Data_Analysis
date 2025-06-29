import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from typing import List, Dict, Tuple, Optional

class TiktokProducer:
    """
    A Kafka producer that publishes TikTok data to Kafka topics.
    Handles Kafka connection, publishing, and error handling.
    """
    
    def __init__(self, 
                 kafka_config: Dict,
                 topic: str = "tiktok-data"):
        """
        Initialize the Kafka Producer.
        
        Args:
            kafka_config (dict): Kafka configuration (bootstrap_servers, etc.)
            topic (str): Kafka topic for TikTok data (posts with comments)
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
        post_key = f"{post.get('profile_name')}_{post.get('post_id')}"
        
        success = self._publish_to_kafka(self.topic, post_key, post)
        
        if success:
            self.published_posts += 1
            self.total_comments_published += len(post.get('comments', []))
            self.logger.info(f"✅ Published post {post.get('post_id')} with {len(post.get('comments', []))} comments")
        else:
            self.failed_posts += 1
            self.logger.error(f"❌ Failed to publish post {post.get('post_id')}")
            
        return success
    
    def publish_posts_batch(self, posts: List[Dict], brand_name: str = "", profile_name: str = "") -> Dict:
        """
        Publish a batch of posts to Kafka.
        
        Args:
            posts (List[Dict]): List of posts with comments to publish
            brand_name (str): Brand name for summary
            profile_name (str): Profile name for summary
            
        Returns:
            Dict: Summary statistics
        """
        self.logger.info(f"🚀 Starting publishing {len(posts)} posts to Kafka")
        
        # Reset counters
        self.published_posts = 0
        self.failed_posts = 0
        self.total_comments_published = 0
        
        try:
            for i, post in enumerate(posts, 1):
                self.logger.info(f"📤 Publishing post {i}/{len(posts)}")
                self.publish_post_with_comments(post)
                
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
        
        summary = self._create_summary(brand_name, profile_name)
        self._print_final_summary(summary)
        
        return summary
    
    def _create_summary(self, brand_name: str = "", profile_name: str = "") -> Dict:
        """Create summary statistics."""
        return {
            'brand_name': brand_name,
            'profile_name': profile_name,
            'collection_time': datetime.now().isoformat(),
            'published_posts': self.published_posts,
            'total_comments_published': self.total_comments_published,
            'failed_posts': self.failed_posts,
            'total_published_messages': self.published_posts,  # Each post+comments = 1 Kafka message
            'success_rate': (self.published_posts / (self.published_posts + self.failed_posts) * 100) 
                           if (self.published_posts + self.failed_posts) > 0 else 0,
            'average_comments_per_post': (self.total_comments_published / self.published_posts) 
                                       if self.published_posts > 0 else 0
        }
    
    def _print_final_summary(self, summary: Dict):
        """Print final summary to console."""
        print("\n" + "="*60)
        print("🎉 TIKTOK DATA PUBLISHING COMPLETED")
        print("="*60)
        print(f"📱 Brand: {summary['brand_name']}")
        print(f"📄 Page: {summary['profile_name']}")  
        print(f"⏰ Completed at: {summary['collection_time']}")
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


# Example usage combining both modules
def main():
    """Example usage of TiktokProcessor and TiktokKafkaProducer."""
    from tiktok_data_processor import TiktokProcessor
    
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],  # Adjust as needed
        'client_id': 'tiktokproducer',
        # Add other Kafka configs as needed (security, etc.)
    }
    
    # Initialize processor
    processor = TiktokProcessor(
        apify_token="",
        brand_name="orangemaroc",
        profile_name="orangemaroc",
        max_posts=2,
        max_comments_per_post=2,
        days_back=100
    )
    
    # Initialize Kafka producer
    producer = TiktokProducer(
        kafka_config=kafka_config,
        topic="tiktok-data-test"
    )
    
    try:
        # Step 1: Process and collect data
        posts = processor.collect_posts_with_comments(topic="tiktok-data-test")
        
        # Step 2: Publish to Kafka
        if posts:
            summary = producer.publish_posts_batch(
                posts=posts,
                brand_name=processor.brand_name,
                profile_name=processor.profile_name
            )
        else:
            print("❌ No posts to publish")
            
    finally:
        # Always close the producer
        producer.close()


if __name__ == "__main__":
    main()