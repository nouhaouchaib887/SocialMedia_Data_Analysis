from Src.Kafka.BaseKafkaConsumer import BaseKafkaConsumer
import json
import uuid
from datetime import datetime
import logging

class TrustpilotKafkaConsumer(BaseKafkaConsumer):
    """
    Consumer class specifically for Trustpilot reviews that:
    - Processes Trustpilot reviews from Kafka
    - Transforms data to match the MongoDB schema
    - Stores results in MongoDB 'reviews' collection
    """
    
    def __init__(self, 
                 kafka_config, 
                 topics, 
                 sentiment_analyzer=None,
                 mongodb_uri="mongodb://localhost:27017/", 
                 db_name="social_media_db",
                 collection_name="reviews"):
        """
        Initialize the TrustpilotConsumer.
        
        Args:
            kafka_config (dict): Kafka consumer configuration
            topics (list): List of Kafka topics to consume from
            sentiment_analyzer: Optional sentiment analyzer for enriching data
            mongodb_uri (str): MongoDB connection URI
            db_name (str): MongoDB database name
            collection_name (str): MongoDB collection name
        """
        super().__init__(
            kafka_config=kafka_config,
            topics=topics,
            mongodb_uri=mongodb_uri,
            db_name=db_name,
            collection_name=collection_name,
            consumer_name="TrustpilotConsumer"
        )
        self.sentiment_analyzer = sentiment_analyzer
        self.logger.info("TrustpilotConsumer initialized")
    
    def _process_data(self, data):
        """
        Process Trustpilot review data from Kafka messages.
        
        Args:
            data (dict): Parsed JSON data from Kafka message
            
        Returns:
            list: List of processed review items
        """
        processed_items = []
        
        # Check if the message contains a batch of reviews
        if 'reviews' in data and isinstance(data['reviews'], list):
            self.logger.info(f"Processing batch of {len(data['reviews'])} reviews for brand: {data.get('brand', 'unknown')}")
            
            # Process each review in the batch
            for review in data['reviews']:
                processed_item = self.enrich_data(review)
                if processed_item:
                    processed_items.append(processed_item)
        
        # If it's a single review
        elif 'Rating' in data or 'rating' in data:
            self.logger.info(f"Processing single review for brand: {data.get('brand', 'unknown')}")
            processed_item = self.enrich_data(data)
            if processed_item:
                processed_items.append(processed_item)
        
        # If it's statistics data, skip it
        elif 'statistics' in data or 'stats' in data:
            self.logger.info("Skipping statistics data")
            return []
        
        # Unknown format
        else:
            self.logger.warning(f"Unknown data format: {data.keys()}")
            return []
        
        return processed_items
    
    def enrich_data(self, item):
        """
        Enrich and transform Trustpilot review data to match the MongoDB schema.
        
        Args:
            item: Raw review data item
            
        Returns:
            dict: Enriched and transformed data item
        """
        try:
            # Extract rating (could be 'Rating' or 'rating')
            rating = item.get('Rating', item.get('rating'))
            if rating is None:
                self.logger.warning("Review missing rating, skipping")
                return None
            
            # Extract review content (could be 'Review' or 'review')
            review_content = item.get('Review', item.get('review', item.get('review_content')))
            if not review_content:
                self.logger.warning("Review missing content, skipping")
                return None
            
            # Extract and parse date
            date_str = item.get('Parsed Date', item.get('parsed_date', item.get('review_date')))
            if not date_str:
                # Default to current date if not provided
                review_date = datetime.now()
                self.logger.warning("Review missing date, using current date")
            else:
                try:
                    # Try to parse the date
                    if isinstance(date_str, str):
                        review_date = datetime.strptime(date_str, '%Y-%m-%d')
                    else:
                        review_date = date_str
                except ValueError:
                    # If parsing fails, use current date
                    review_date = datetime.now()
                    self.logger.warning(f"Failed to parse date: {date_str}, using current date")
            
            # Generate a review ID if not present
            review_id = item.get('review_id', str(uuid.uuid4()))
            
            # Create the transformed item
            transformed_item = {
                'platform': 'trustpilot',
                'brand': item.get('brand', 'unknown'),
                'review_id': review_id,
                'rating': int(rating),
                'review_content': review_content,
                'review_date': review_date,
                # Add default sentiment and categories (will be updated if analyzer available)
                'sentiment': 'neutral',
                'categories': [],
                'products_services': []
            }
            
            # Apply sentiment analysis if available
            if self.sentiment_analyzer and review_content:
                try:
                    analysis_result = self.sentiment_analyzer.generate_telecom_analysis(review_content)
                    if isinstance(analysis_result, dict):
                        # Le résultat est déjà un dictionnaire, on peut l'utiliser directement
                        transformed_item['sentiment'] = analysis_result.get('sentiment', 'neutral')
                        transformed_item['categories'] = analysis_result.get('categories', [])
                        transformed_item['products_services'] = analysis_result.get('produits_ou_services', [])
                    elif isinstance(analysis_result, str):
                        # Si c'est une chaîne, essayer de la parser comme JSON
                        try:
                            analysis_json = json.loads(analysis_result)
                            transformed_item['sentiment'] = analysis_json.get('sentiment', 'neutral')
                            transformed_item['categories'] = analysis_json.get('categories', [])
                            transformed_item['products_services'] = analysis_json.get('produits_ou_services', [])
                        except json.JSONDecodeError:
                            self.logger.error("Failed to parse sentiment analysis result as JSON")
                except Exception as e:
                    self.logger.error(f"Error during sentiment analysis: {str(e)}")
            
            return transformed_item
            
        except Exception as e:
            self.logger.error(f"Error enriching data: {e}")
            return None
    
    def save_to_mongodb(self, data):
        """
        Save reviews to MongoDB with validation against the schema.
        
        Args:
            data (list): List of enriched review items
            
        Returns:
            int: Number of documents inserted
        """
        if not data or self.collection is None:
            return 0
            
        try:
            # Validate each document before insertion
            valid_documents = []
            for doc in data:
                # Ensure required fields are present
                if all(key in doc for key in ['platform', 'brand', 'review_id', 'rating', 'review_content', 'review_date']):
                    valid_documents.append(doc)
                else:
                    missing = [key for key in ['platform', 'brand', 'review_id', 'rating', 'review_content', 'review_date'] if key not in doc]
                    self.logger.warning(f"Document missing required fields {missing}, skipping")
            
            # Insert valid documents
            if valid_documents:
                result = self.collection.insert_many(valid_documents)
                inserted_count = len(result.inserted_ids)
                self.logger.info(f"Inserted {inserted_count} documents into MongoDB")
                return inserted_count
            else:
                self.logger.warning("No valid documents to insert")
                return 0
                
        except Exception as e:
            self.logger.error(f"Error saving to MongoDB: {e}")
            return 0