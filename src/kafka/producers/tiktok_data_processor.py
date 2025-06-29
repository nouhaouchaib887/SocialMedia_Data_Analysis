import json
import time
from datetime import datetime
from typing import List, Dict, Optional
from collectors.tiktok_scraper.tiktok_apify_collector import TiktokAPifyCollector
import logging

class TiktokProcessor:
    """
    A processor that collects TikTok data and enriches it with metadata.
    Handles data collection, enrichment, and processing logic.
    """
    
    def __init__(self, 
                 apify_token: str,
                 brand_name: str,
                 profile_name: str,
                 max_posts: int = 2,
                 max_comments_per_post: int = 3,
                 days_back: int = 15):
        """
        Initialize the TikTok Processor.
        
        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for TikTok page
            profile_name (str): TikTok page name
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            days_back (int): Number of days to go back for post collection
        """
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize TikTok collector
        self.collector = TiktokAPifyCollector(
            apify_token=apify_token,
            brand_name=brand_name,
            profile_name=profile_name,
            max_posts=max_posts,
            max_comments_per_post=max_comments_per_post,
            days_back=days_back
        )
        
        # Store configuration
        self.brand_name = brand_name
        self.profile_name = profile_name
        
    def _enrich_post_data(self, post: Dict, topic: str = "tiktok-data") -> Dict:
        """
        Enrich post data with additional metadata.
        
        Args:
            post (dict): Original post data
            topic (str): Kafka topic name for metadata
            
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
                'message_type': 'tiktok_post_with_comments',
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
    
    def collect_posts_with_comments(self, topic: str = "tiktok-data") -> List[Dict]:
        """
        Collect TikTok posts with comments and enrich them.
        
        Args:
            topic (str): Kafka topic name for metadata
            
        Returns:
            List[Dict]: List of enriched posts with comments
        """
        self.logger.info(f"🚀 Starting TikTok data collection for {self.brand_name}")
        
        enriched_posts = []
        
        try:
            # Collect posts
            posts = self.collector.collect_posts()
            
            if not posts:
                self.logger.warning("❌ No posts collected")
                return enriched_posts
            
            self.logger.info(f"📊 Processing {len(posts)} posts...")
            
            for i, post in enumerate(posts, 1):
                if i >= 0:
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
                
                    # Enrich the post data
                    enriched_post = self._enrich_post_data(post, topic)
                    enriched_posts.append(enriched_post)
                
                    # Add delay to avoid rate limiting (except for last post)
                    if i < len(posts):
                        self.logger.info("⏳ Waiting 10 seconds before next post...")
                        time.sleep(10)
        
        except Exception as e:
            self.logger.error(f"❌ Error during collection: {str(e)}")
        
        self.logger.info(f"✅ Processed {len(enriched_posts)} posts with comments")
        return enriched_posts