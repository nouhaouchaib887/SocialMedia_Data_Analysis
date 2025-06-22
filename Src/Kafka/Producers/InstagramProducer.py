import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import os
from typing import List, Dict, Tuple, Optional
from collectors.Instargram.InstagramAPifyCollector import InstagramAPifyCollector
# Import your existing InstagramCollector
# from your_module import InstagramCollector

class InstagramProducer:
    """
    A Kafka producer that collects Instagram data and publishes it to Kafka topics.
    Also saves data to JSON files as backup.
    """
    
    def __init__(self, 
                 kafka_config: Dict,
                 apify_token: str,
                 brand_name: str,
                 user_name: str,
                 topic: str = "instagram-data",
                 max_posts: int = 2,
                 max_comments_per_post: int = 3,
                 days_back: int = 15):
        """
        Initialize the Instagram Producer.
        
        Args:
            kafka_config (dict): Kafka configuration (bootstrap_servers, etc.)
            apify_token (str): Apify API token
            brand_name (str): Brand name for Instagram page
            page_name (str): Instagram page name
            topic (str): Kafka topic for Instagram data (posts with comments)
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            days_back (int): Number of days to go back for post collection
            backup_dir (str): Directory to save backup JSON files
        """
        self.kafka_config = kafka_config
        self.topic = topic
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        # Initialize Instagram collector
        self.collector = InstagramAPifyCollector(
            apify_token=apify_token,
            brand_name=brand_name,
            user_name=user_name,
            max_posts=max_posts,
            max_comments_per_post=max_comments_per_post,
            days_back=days_back
        )
        
        
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
    def _enrich_post_data(self, post: Dict) -> Dict:
        """
        Enrich post data with additional metadata for Kafka.
        
        Args:
            post (dict): Original post data
            
        Returns:
            dict: Enriched post data
        """
        # Helper function to safely get numeric values
        def safe_int(value, default=0):
            try:
                return int(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        # Helper function to safely get list length
        def safe_list_len(value):
            try:
                return len(value) if value else 0
            except (TypeError, AttributeError):
                return 0
        
        # Helper function to safely get string length
        def safe_str_len(value):
            try:
                return len(str(value)) if value is not None else 0
            except (TypeError, AttributeError):
                return 0
        
        enriched_post = {
            **post,
            # Kafka metadata
            'kafka_metadata': {
                'topic': self.topic,
                'produced_at': datetime.utcnow().isoformat(),
                'producer_timestamp': int(time.time() * 1000),
                'message_type': 'instagram_post_with_comments',
                'version': '1.0'
            },
            # Collection metadata
            'collection_params': {
                    'max_posts': self.collector.max_posts,
                    'max_comments_per_post': self.collector.max_comments_per_post,
                    'days_back': self.collector.days_back,
                    'from_date': getattr(self.collector, 'from_date', None)
                }
            }

        
        
        # Enrich comments with additional metadata
        if 'comments' in enriched_post and enriched_post['comments']:
            enriched_comments = []
            for comment in enriched_post['comments']:
                enriched_comment = {
                    **comment,
                    'comment_metadata': {
                        'parent_post_id': str(post.get('post_id', '')),
                        'comment_engagement_score': (safe_int(comment.get('like_count', 0)) + 
                                                   safe_int(comment.get('reply_count', 0))),
                        'comment_length': safe_str_len(comment.get('message', '')),
                        'has_hashtags': safe_list_len(comment.get('hashtags', [])) > 0,
                        'has_mentions': safe_list_len(comment.get('mentions', [])) > 0
                    }
                }
                enriched_comments.append(enriched_comment)
            enriched_post['comments'] = enriched_comments
        
        return enriched_post
        
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
        # Enrich the post data
        enriched_post = self._enrich_post_data(post)
        
        post_key = f"{post.get('page_name')}_{post.get('post_id')}"
        
        success = self._publish_to_kafka(self.topic, post_key, enriched_post)
        
        if success:
            self.published_posts += 1
            self.total_comments_published += len(post.get('comments', []))
            self.logger.info(f"✅ Published post {post.get('post_id')} with {len(post.get('comments', []))} comments")
        else:
            self.failed_posts += 1
            self.logger.error(f"❌ Failed to publish post {post.get('post_id')}")
            
        return success
    
    def collect_and_publish(self) -> Tuple[Dict, List[str]]:
        """
        Collect Instagram data and publish to Kafka with backup.
        
        Returns:
            tuple: (summary_stats, backup_files)
        """
        self.logger.info(f"🚀 Starting Instagram data collection and publishing for {self.collector.brand_name}")
        
        # Reset counters
        self.published_posts = 0
        self.failed_posts = 0
        self.total_comments_published = 0
        
        
        try:
            # Collect posts one by one and publish immediately with comments
            posts = self.collector.collect_posts()
            
            if not posts:
                self.logger.warning("❌ No posts collected")
                return self._create_summary()
            
            self.logger.info(f"📊 Processing {len(posts)} posts...")
            
            for i, post in enumerate(posts, 1):
                self.logger.info(f"\n--- Processing Post {i}/{len(posts)} ---")
                
                # Collect comments for this post first
                if post.get("permalink"):
                    try:
                        self.logger.info(f"🔍 Collecting comments for post {post.get('post_id')}...")
                        comments = self.collector.collect_comments_for_post(post["permalink"])
                        post["comments"] = comments  # Add comments to the post
                        
                        if comments:
                            self.logger.info(f"✅ {len(comments)} comments collected for post {post.get('post_id')}")
                        else:
                            self.logger.info(f"ℹ️ No comments found for post {post.get('post_id')}")
                            
                    except Exception as e:
                        self.logger.error(f"❌ Error collecting comments for post {post.get('post_id')}: {str(e)}")
                        post["comments"] = []  # Ensure comments field exists even if empty
                else:
                    self.logger.warning(f"⚠️ No permalink available for post {post.get('post_id')}")
                    post["comments"] = []
                
                # Now publish the complete post with comments
                post_published = self.publish_post_with_comments(post)
               
                
                # Add delay to avoid rate limiting (except for last post)
                if i < len(posts):
                    self.logger.info("⏳ Waiting 10 seconds before next post...")
                    time.sleep(10)
        
        except Exception as e:
            self.logger.error(f"❌ Error during collection: {str(e)}")
        
        finally:
            
            
            # Flush and close Kafka producer
            if self.producer:
                try:
                    self.producer.flush(timeout=30)
                    self.logger.info("✅ Kafka producer flushed successfully")
                except Exception as e:
                    self.logger.error(f"⚠️ Error flushing Kafka producer: {str(e)}")
        
        summary = self._create_summary()
        self._print_final_summary(summary)
        
        return summary
    def _create_summary(self) -> Dict:
        """Create summary statistics."""
        return {
            'brand_name': self.collector.brand_name,
            'page_name': self.collector.user_name,
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
        print("🎉 Instagram DATA COLLECTION & PUBLISHING COMPLETED")
        print("="*60)
        print(f"📱 Brand: {summary['brand_name']}")
        print(f"📄 Page: {summary['page_name']}")  
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


# Example usage
def main():
    """Example usage of InstagramProducer."""
    
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],  # Adjust as needed
        'client_id': 'instagramproducer',
        # Add other Kafka configs as needed (security, etc.)
    }
    
    # Initialize producer
    producer = InstagramProducer(
        kafka_config=kafka_config,
        apify_token="apify_api_c84soB2gwlEwt3wzLhENao0KardhjD42qd0j",
        brand_name="maroctelecom",
        user_name="maroctelecom",
        topic="instagram-data-test" , # Single topic for posts with comments
        max_posts=2,
        max_comments_per_post=2,
        days_back=15
    )
    
    try:
        # Run collection and publishing
        summary = producer.collect_and_publish()
        
        

            
    finally:
        # Always close the producer
        producer.close()


if __name__ == "__main__":
    main()