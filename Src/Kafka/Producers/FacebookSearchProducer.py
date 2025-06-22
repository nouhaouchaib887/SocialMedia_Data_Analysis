import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import os
from typing import List, Dict, Tuple, Optional
from collectors.Facebook.FacebookSearchCollector import FacebookSearchCollector


class FacebookSearchProducer:
    """
    A Kafka producer that collects Facebook data using search queries and publishes it to Kafka topics.
    Also saves data to JSON files as backup.
    """
    
    def __init__(self, 
                 kafka_config: Dict,
                 apify_token: str,
                 brand_name: str,
                 search_query: Optional[str] = None,
                 page_name: Optional[str] = None,
                 topic: str = "facebook-search-data",
                 max_posts: int = 2,
                 max_comments_per_post: int = 3,
                 post_time_range: str = "30d",
                 backup_dir: str = "facebook_search_data_backup"):
        """
        Initialize the FacebookSearchProducer.
        
        Args:
            kafka_config (dict): Kafka configuration (bootstrap_servers, etc.)
            apify_token (str): Apify API token
            brand_name (str): Brand name for identification
            search_query (str, optional): Search query for posts (e.g., '#orangemaroc', 'orangemaroc')
            page_name (str, optional): Page name for direct page scraping (alternative to search)
            topic (str): Kafka topic for Facebook search data
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            post_time_range (str): Time range for posts (e.g., '30d', '7d', '90d')
            backup_dir (str): Directory to save backup JSON files
        """
        self.kafka_config = kafka_config
        self.topic = topic
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize Facebook search collector
        self.collector = FacebookSearchCollector(
            apify_token=apify_token,
            brand_name=brand_name,
            search_query=search_query,
            page_name=page_name,
            max_posts=max_posts,
            max_comments_per_post=max_comments_per_post,
            post_time_range=post_time_range
        )
        
        # Initialize Kafka producer
        self.producer = None
        self._init_kafka_producer()
        
        # Create backup directory
        os.makedirs(backup_dir, exist_ok=True)
        
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
                'message_type': 'facebook_search_post_with_comments',
                'version': '1.0'
            },
            # Collection metadata
            
            'collection_params': {
                    'search_query': self.collector.search_query,
                    'max_posts': self.collector.max_posts,
                    'max_comments_per_post': self.collector.max_comments_per_post,
                    'post_time_range': self.collector.post_time_range
                
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
        
        post_key = f"{post.get('brand_name')}_{post.get('post_id')}"
        
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
        Collect Facebook search data and publish to Kafka with backup.
        
        Returns:
            tuple: (summary_stats, backup_files)
        """
        search_info = f"query: '{self.collector.search_query}'" if self.collector.search_query else f"page: '{self.collector.page_name}'"
        self.logger.info(f"🚀 Starting Facebook search data collection and publishing for {self.collector.brand_name} ({search_info})")
        
        # Reset counters
        self.published_posts = 0
        self.failed_posts = 0
        self.total_comments_published = 0
        
       
        
        try:
            # Collect all data using the FacebookSearchCollector
            posts = self.collector.collect_all_data()
            
            if not posts:
                self.logger.warning("❌ No posts collected")
                return self._create_summary()
            
            self.logger.info(f"📊 Processing {len(posts)} posts...")
            
            for i, post in enumerate(posts, 1):
                self.logger.info(f"\n--- Processing Post {i}/{len(posts)} ---")
                
                # Publish the post with comments (comments are already included by FacebookSearchCollector)
                post_published = self.publish_post_with_comments(post)
               
                
                # Add delay to avoid overwhelming Kafka (except for last post)
                if i < len(posts):
                    self.logger.info("⏳ Waiting 2 seconds before next post...")
                    time.sleep(2)
                    
        
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
            'search_query': self.collector.search_query,
            'page_name': self.collector.page_name,
            'post_time_range': self.collector.post_time_range,
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
        print("🎉 FACEBOOK SEARCH DATA COLLECTION & PUBLISHING COMPLETED")
        print("="*60)
        print(f"📱 Brand: {summary['brand_name']}")
        if summary['search_query']:
            print(f"🔍 Search Query: {summary['search_query']}")
        if summary['page_name']:
            print(f"📄 Page: {summary['page_name']}")
        print(f"⏰ Time Range: {summary['post_time_range']}")
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
    """Example usage of FacebookSearchProducer."""
    
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],  # Adjust as needed
        'client_id': 'facebook-search-producer',
        # Add other Kafka configs as needed (security, etc.)
    }
    
    
    # Example :  Search by #brand name
    producer_search= FacebookSearchProducer(
        kafka_config=kafka_config,
        apify_token="apify_api_c84soB2gwlEwt3wzLhENao0KardhjD42qd0j",
        brand_name="orangemaroc",
        search_query="#orangemaroc",
        topic="facebook-search-data-test",
        max_posts=2,
        max_comments_per_post=2,
        post_time_range="30d"
    )
    
    try:
        # Run collection and publishing for hashtag search
        print("=== HASHTAG SEARCH COLLECTION ===")
        summary1 = producer_search.collect_and_publish()
        
        
        
            
    finally:
        # Always close the producers
        producer_search.close()


if __name__ == "__main__":
    main()