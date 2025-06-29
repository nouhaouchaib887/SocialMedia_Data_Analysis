import json
import time
from datetime import datetime
import logging
import os
from typing import List, Dict, Tuple, Optional
from collectors.facebook_scraper.facebook_search_collector import FacebookSearchCollector


class FacebookSearchProcessor:
    """
    A processor that collects Facebook data using search queries and processes it.
    Handles data enrichment and preparation for publishing.
    """
    
    def __init__(self, 
                 apify_token: str,
                 brand_name: str,
                 search_query: Optional[str] = None,
                 max_posts: int = 2,
                 max_comments_per_post: int = 3,
                 post_time_range: str = "30d",
                 backup_dir: str = "facebook_search_data_backup"):
        """
        Initialize the FacebookSearchProcessor.
        
        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for identification
            search_query (str, optional): Search query for posts (e.g., '#orangemaroc', 'orangemaroc')
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            post_time_range (str): Time range for posts (e.g., '30d', '7d', '90d')
            backup_dir (str): Directory to save backup JSON files
        """
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize Facebook search collector
        self.collector = FacebookSearchCollector(
            apify_token=apify_token,
            brand_name=brand_name,
            search_query=search_query,
            max_posts=max_posts,
            max_comments_per_post=max_comments_per_post,
            post_time_range=post_time_range
        )
        
        # Create backup directory
        os.makedirs(backup_dir, exist_ok=True)
        
        # Initialize counters
        self.processed_posts = 0
        self.failed_posts = 0
        self.total_comments_processed = 0
        
    def _enrich_post_data(self, post: Dict, topic: str = "facebook-search-data") -> Dict:
        """
        Enrich post data with additional metadata.
        
        Args:
            post (dict): Original post data
            topic (str): Topic name for metadata
            
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
                'topic': topic,
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
    
    def process_post_with_comments(self, post: Dict, topic: str = "facebook-search-data") -> Dict:
        """
        Process a post with its comments.
        
        Args:
            post (dict): Post data with comments
            topic (str): Topic name for metadata
            
        Returns:
            dict: Processed and enriched post data
        """
        try:
            # Enrich the post data
            enriched_post = self._enrich_post_data(post, topic)
            
            self.processed_posts += 1
            self.total_comments_processed += len(post.get('comments', []))
            self.logger.info(f"✅ Processed post {post.get('post_id')} with {len(post.get('comments', []))} comments")
            
            return enriched_post
            
        except Exception as e:
            self.failed_posts += 1
            self.logger.error(f"❌ Failed to process post {post.get('post_id')}: {str(e)}")
            return None
    
    def collect_and_process_all(self, topic: str = "facebook-search-data") -> Tuple[List[Dict], Dict]:
        """
        Collect Facebook search data and process all posts.
        
        Args:
            topic (str): Topic name for metadata
            
        Returns:
            tuple: (processed_posts_list, summary_stats)
        """
        search_info = f"query: '{self.collector.search_query}'" if self.collector.search_query else f"page: '{self.collector.page_name}'"
        self.logger.info(f"🚀 Starting Facebook search data collection and processing for {self.collector.brand_name} ({search_info})")
        
        # Reset counters
        self.processed_posts = 0
        self.failed_posts = 0
        self.total_comments_processed = 0
        
        processed_posts = []
        
        try:
            # Collect all data using the FacebookSearchCollector
            posts = self.collector.collect_all_data()
            
            if not posts:
                self.logger.warning("❌ No posts collected")
                return processed_posts, self._create_summary()
            
            self.logger.info(f"📊 Processing {len(posts)} posts...")
            
            for i, post in enumerate(posts, 1):
                self.logger.info(f"\n--- Processing Post {i}/{len(posts)} ---")
                
                # Process the post with comments
                processed_post = self.process_post_with_comments(post, topic)
                
                if processed_post:
                    processed_posts.append(processed_post)
                
                # Add delay to avoid overwhelming (except for last post)
                if i < len(posts):
                    self.logger.info("⏳ Waiting 2 seconds before next post...")
                    time.sleep(2)
                    
        except Exception as e:
            self.logger.error(f"❌ Error during collection: {str(e)}")
        
        summary = self._create_summary()
        self._print_processing_summary(summary)
        
        return processed_posts, summary
    
    def _create_summary(self) -> Dict:
        """Create summary statistics."""
        return {
            'brand_name': self.collector.brand_name,
            'search_query': self.collector.search_query,
            'page_name': self.collector.page_name,
            'post_time_range': self.collector.post_time_range,
            'processing_time': datetime.now().isoformat(),
            'processed_posts': self.processed_posts,
            'total_comments_processed': self.total_comments_processed,
            'failed_posts': self.failed_posts,
            'success_rate': (self.processed_posts / (self.processed_posts + self.failed_posts) * 100) 
                           if (self.processed_posts + self.failed_posts) > 0 else 0,
            'average_comments_per_post': (self.total_comments_processed / self.processed_posts) 
                                       if self.processed_posts > 0 else 0
        }
    
    def _print_processing_summary(self, summary: Dict):
        """Print processing summary to console."""
        print("\n" + "="*60)
        print("🔄 FACEBOOK SEARCH DATA PROCESSING COMPLETED")
        print("="*60)
        print(f"📱 Brand: {summary['brand_name']}")
        if summary['search_query']:
            print(f"🔍 Search Query: {summary['search_query']}")
        if summary['page_name']:
            print(f"📄 Page: {summary['page_name']}")
        print(f"⏰ Time Range: {summary['post_time_range']}")
        print(f"⏰ Processed at: {summary['processing_time']}")
        print("\n📊 PROCESSING STATISTICS:")
        print(f"   ✅ Posts processed: {summary['processed_posts']}")
        print(f"   💬 Total comments processed: {summary['total_comments_processed']}")
        print(f"   📊 Average comments per post: {summary['average_comments_per_post']:.1f}")
        print(f"   ❌ Posts failed: {summary['failed_posts']}")
        print(f"   🎯 Success rate: {summary['success_rate']:.1f}%")
        print("="*60)