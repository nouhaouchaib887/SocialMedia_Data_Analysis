import json
import time
from datetime import datetime
import logging
import os
from typing import List, Dict, Tuple, Optional
from collectors.instagram_scraper.instagram_apify_collector import InstagramAPifyCollector


class InstagramProcessor:
    """
    A processor that collects Instagram data and enriches it with metadata.
    Handles data collection and processing logic.
    """
    
    def __init__(self, 
                 apify_token: str,
                 brand_name: str,
                 user_name: str,
                 max_posts: int = 2,
                 max_comments_per_post: int = 3,
                 days_back: int = 15):
        """
        Initialize the Instagram Processor.
        
        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for Instagram page
            user_name (str): Instagram page name
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            days_back (int): Number of days to go back for post collection
        """
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
        
        # Initialize counters
        self.processed_posts = 0
        self.failed_posts = 0
        self.total_comments_processed = 0
        
    def _enrich_post_data(self, post: Dict, topic: str = "instagram-data") -> Dict:
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
    
    def process_post_with_comments(self, post: Dict, topic: str = "instagram-data") -> Optional[Dict]:
        """
        Process a post with its comments.
        
        Args:
            post (dict): Post data
            topic (str): Topic name for metadata
            
        Returns:
            dict: Processed and enriched post data, or None if failed
        """
        try:
            # Collect comments for this post if permalink exists
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
            
            self.processed_posts += 1
            self.total_comments_processed += len(post.get('comments', []))
            self.logger.info(f"✅ Processed post {post.get('post_id')} with {len(post.get('comments', []))} comments")
            
            return enriched_post
            
        except Exception as e:
            self.failed_posts += 1
            self.logger.error(f"❌ Failed to process post {post.get('post_id')}: {str(e)}")
            return None
    
    def collect_and_process(self, topic: str = "instagram-data") -> Tuple[List[Dict], Dict]:
        """
        Collect Instagram data and process it.
        
        Args:
            topic (str): Topic name for metadata
            
        Returns:
            tuple: (processed_posts_list, summary_stats)
        """
        self.logger.info(f"🚀 Starting Instagram data collection and processing for {self.collector.brand_name}")
        
        # Reset counters
        self.processed_posts = 0
        self.failed_posts = 0
        self.total_comments_processed = 0
        
        processed_posts_list = []
        
        try:
            # Collect posts
            posts = self.collector.collect_posts()
            
            if not posts:
                self.logger.warning("❌ No posts collected")
                return processed_posts_list, self._create_summary()
            
            self.logger.info(f"📊 Processing {len(posts)} posts...")
            
            for i, post in enumerate(posts, 1):
                self.logger.info(f"\n--- Processing Post {i}/{len(posts)} ---")
                
                # Process the post with comments
                processed_post = self.process_post_with_comments(post, topic)
                
                if processed_post:
                    processed_posts_list.append(processed_post)
                
                # Add delay to avoid rate limiting (except for last post)
                if i < len(posts):
                    self.logger.info("⏳ Waiting 10 seconds before next post...")
                    time.sleep(10)
        
        except Exception as e:
            self.logger.error(f"❌ Error during collection: {str(e)}")
        
        summary = self._create_summary()
        self._print_processing_summary(summary)
        
        return processed_posts_list, summary
    
    def _create_summary(self) -> Dict:
        """Create summary statistics."""
        return {
            'brand_name': self.collector.brand_name,
            'page_name': self.collector.user_name,
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
        print("🎉 Instagram DATA PROCESSING COMPLETED")
        print("="*60)
        print(f"📱 Brand: {summary['brand_name']}")
        print(f"📄 Page: {summary['page_name']}")  
        print(f"⏰ Completed at: {summary['processing_time']}")
        print("\n📊 PROCESSING STATISTICS:")
        print(f"   ✅ Posts processed: {summary['processed_posts']}")
        print(f"   💬 Total comments processed: {summary['total_comments_processed']}")
        print(f"   📊 Average comments per post: {summary['average_comments_per_post']:.1f}")
        print(f"   ❌ Posts failed: {summary['failed_posts']}")
        print(f"   🎯 Success rate: {summary['success_rate']:.1f}%")
        print("="*60)