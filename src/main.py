#!/usr/bin/env python3
"""
Main entry point for Social Media Data Pipeline
Supports multiple platforms: Instagram, Facebook, TikTok
"""

import asyncio
import argparse
import logging
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import signal
import os

# Import your producers and consumers
# Assuming you have similar structure for all platforms
from kafka.producers.tiktok_producer import TiktokProducer
from kafka.producers.instagram_producer import InstagramProducer  # Assuming this exists
from kafka.producers.facebook_producer import FacebookProducer    # Assuming this exists

from kafka.consumers.tiktok_consumer import TiktokDataConsumer
from kafka.consumers.instagram_consumer import InstagramDataConsumer  # Assuming this exists
from kafka.consumers.facebook_consumer import FacebookDataConsumer    # Assuming this exists

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

class SocialMediaPipeline:
    """Main pipeline orchestrator for social media data collection and processing."""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialize the pipeline with configuration."""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.running_consumers = []
        self.shutdown_event = asyncio.Event()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.shutdown_event.set()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            sys.exit(1)
    
    def _get_producer_class(self, platform: str):
        """Get the appropriate producer class for the platform."""
        producers = {
            'tiktok': TiktokProducer,
            'instagram': InstagramProducer,
            'facebook': FacebookProducer
        }
        return producers.get(platform.lower())
    
    def _get_consumer_class(self, platform: str):
        """Get the appropriate consumer class for the platform."""
        consumers = {
            'tiktok': TiktokDataConsumer,
            'instagram': InstagramDataConsumer,
            'facebook': FacebookDataConsumer
        }
        return consumers.get(platform.lower())
    
    def run_producer(self, platform: str, brand_config: Dict[str, Any]) -> bool:
        """Run a producer for a specific platform and brand."""
        logger.info(f"Starting {platform} producer for brand: {brand_config['brand_name']}")
        
        try:
            # Get producer class
            ProducerClass = self._get_producer_class(platform)
            if not ProducerClass:
                logger.error(f"Unknown platform: {platform}")
                return False
            
            # Prepare producer configuration
            producer_config = {
                'kafka_config': self.config['kafka'],
                'topic': f"{platform}-data",
                **brand_config,
                **self.config.get('api_keys', {})
            }
            
            # Initialize and run producer
            producer = ProducerClass(**producer_config)
            
            try:
                summary = producer.collect_and_publish()
                logger.info(f"Producer completed successfully for {platform}/{brand_config['brand_name']}")
                logger.info(f"Summary: {summary}")
                return True
            finally:
                producer.close()
                
        except Exception as e:
            logger.error(f"Error running {platform} producer for {brand_config['brand_name']}: {e}")
            return False
    
    async def run_consumer(self, platform: str) -> None:
        """Run a consumer for a specific platform."""
        logger.info(f"Starting {platform} consumer")
        
        try:
            # Get consumer class
            ConsumerClass = self._get_consumer_class(platform)
            if not ConsumerClass:
                logger.error(f"Unknown platform: {platform}")
                return
            
            # Prepare consumer configuration
            kafka_config = self.config['kafka'].copy()
            kafka_config['topic'] = f"{platform}-data"
            kafka_config['group_id'] = f"{platform}-data-consumer-group"
            
            consumer_config = {
                'kafka_config': kafka_config,
                'mongodb_config': self.config['mongodb'],
                'google_api_key': self.config['api_keys']['gemini_key']
            }
            
            # Initialize and run consumer
            consumer = ConsumerClass(**consumer_config)
            self.running_consumers.append(consumer)
            
            # Run consumer until shutdown event is set
            consumer_task = asyncio.create_task(consumer.run())
            shutdown_task = asyncio.create_task(self.shutdown_event.wait())
            
            # Wait for either consumer to finish or shutdown signal
            done, pending = await asyncio.wait(
                [consumer_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            logger.info(f"{platform} consumer stopped")
            
        except Exception as e:
            logger.error(f"Error running {platform} consumer: {e}")
    
    def run_producers_only(self, platforms: Optional[list] = None, brands: Optional[list] = None):
        """Run producers only for specified platforms and brands."""
        logger.info("Running producers only mode")
        
        # Get platforms to process
        target_platforms = platforms or list(self.config['platforms'].keys())
        
        success_count = 0
        total_count = 0
        
        for platform in target_platforms:
            if platform not in self.config['platforms']:
                logger.warning(f"Platform {platform} not configured, skipping")
                continue
            
            platform_config = self.config['platforms'][platform]
            target_brands = brands or list(platform_config['brands'].keys())
            
            for brand_name in target_brands:
                if brand_name not in platform_config['brands']:
                    logger.warning(f"Brand {brand_name} not configured for {platform}, skipping")
                    continue
                
                brand_config = platform_config['brands'][brand_name]
                brand_config['brand_name'] = brand_name
                
                total_count += 1
                if self.run_producer(platform, brand_config):
                    success_count += 1
        
        logger.info(f"Producers completed: {success_count}/{total_count} successful")
        return success_count == total_count
    
    async def run_consumers_only(self, platforms: Optional[list] = None):
        """Run consumers only for specified platforms."""
        logger.info("Running consumers only mode")
        
        # Get platforms to process
        target_platforms = platforms or list(self.config['platforms'].keys())
        
        # Start all consumers concurrently
        consumer_tasks = []
        for platform in target_platforms:
            if platform in self.config['platforms']:
                task = asyncio.create_task(self.run_consumer(platform))
                consumer_tasks.append(task)
            else:
                logger.warning(f"Platform {platform} not configured, skipping consumer")
        
        if consumer_tasks:
            logger.info(f"Starting {len(consumer_tasks)} consumers")
            await asyncio.gather(*consumer_tasks, return_exceptions=True)
        else:
            logger.warning("No consumers to start")
    
    async def run_full_pipeline(self, platforms: Optional[list] = None, brands: Optional[list] = None):
        """Run the complete pipeline: producers first, then consumers."""
        logger.info("Running full pipeline mode")
        
        # Step 1: Run all producers
        logger.info("Phase 1: Running producers")
        producers_success = self.run_producers_only(platforms, brands)
        
        if not producers_success:
            logger.warning("Some producers failed, but continuing with consumers")
        
        # Step 2: Run consumers
        logger.info("Phase 2: Running consumers")
        await self.run_consumers_only(platforms)
    
    async def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up pipeline resources...")
        
        # Signal all consumers to stop
        self.shutdown_event.set()
        
        # Wait a bit for consumers to clean up
        await asyncio.sleep(2)
        
        logger.info("Pipeline cleanup completed")


def create_default_config():
    """Create a default configuration file."""
    default_config = {
        'kafka': {
            'bootstrap_servers': ['localhost:9092'],
            'client_id': 'social-media-pipeline',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True
        },
        'mongodb': {
            'connection_string': 'mongodb://localhost:27017/',
            'database': 'social_media_db'
        },
        'api_keys': {
            'gemini_key': 'your-gemini-api-key-here',
            'apify_token': 'your-apify-token-here'
        },
        'platforms': {
            'tiktok': {
                'brands': {
                    'orangemaroc': {
                        'profile_name': 'orangemaroc',
                        'max_posts': 10,
                        'max_comments_per_post': 5,
                        'days_back': 30
                    },
                    'brand2': {
                        'profile_name': 'brand2_profile',
                        'max_posts': 5,
                        'max_comments_per_post': 3,
                        'days_back': 7
                    }
                }
            },
            'instagram': {
                'brands': {
                    'orangemaroc': {
                        'profile_name': 'orangemaroc',
                        'max_posts': 15,
                        'max_comments_per_post': 10,
                        'days_back': 30
                    }
                }
            },
            'facebook': {
                'brands': {
                    'orangemaroc': {
                        'page_name': 'Orange Maroc',
                        'max_posts': 20,
                        'max_comments_per_post': 15,
                        'days_back': 30
                    }
                }
            }
        }
    }
    
    config_dir = Path('config')
    config_dir.mkdir(exist_ok=True)
    
    config_file = config_dir / 'pipeline_config.yaml'
    with open(config_file, 'w') as f:
        yaml.dump(default_config, f, default_flow_style=False, indent=2)
    
    print(f"Default configuration created at: {config_file}")
    print("Please edit this file with your actual configuration values.")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Social Media Data Pipeline')
    parser.add_argument('mode', choices=['producers', 'consumers', 'full', 'create-config'], 
                       help='Pipeline mode to run')
    parser.add_argument('--platforms', nargs='+', 
                       choices=['tiktok', 'instagram', 'facebook'],
                       help='Specific platforms to process')
    parser.add_argument('--brands', nargs='+',
                       help='Specific brands to process')
    parser.add_argument('--config', default='config/pipeline_config.yaml',
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    # Handle config creation
    if args.mode == 'create-config':
        create_default_config()
        return
    
    # Initialize pipeline
    try:
        pipeline = SocialMediaPipeline(args.config)
    except SystemExit:
        return
    
    try:
        if args.mode == 'producers':
            success = pipeline.run_producers_only(args.platforms, args.brands)
            sys.exit(0 if success else 1)
        
        elif args.mode == 'consumers':
            await pipeline.run_consumers_only(args.platforms)
        
        elif args.mode == 'full':
            await pipeline.run_full_pipeline(args.platforms, args.brands)
    
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        sys.exit(1)
    finally:
        await pipeline.cleanup()


if __name__ == "__main__":
    asyncio.run(main())