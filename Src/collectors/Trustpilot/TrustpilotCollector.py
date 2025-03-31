import requests
from bs4 import BeautifulSoup
import time
import json
import os
import random
from datetime import datetime


class TrustpilotCollector:
    def __init__(self, brand_name, output_dir="data", delay_range=(1, 3)):
        """
        Initialize the Trustpilot scraper.
        
        Args:
            brand_name (str): Name of the brand to scrape on Trustpilot
            output_dir (str): Directory to save output data
            delay_range (tuple): Range of seconds to delay between requests (min, max)
        """
        self.brand_name = brand_name
        self.brand_url = f"https://www.trustpilot.com/review/{brand_name}"
        self.output_dir = output_dir
        self.delay_range = delay_range
        
        # Create data directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Setup session with headers to mimic browser
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.trustpilot.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _random_delay(self):
        """Add random delay between requests to avoid being blocked"""
        delay = random.uniform(self.delay_range[0], self.delay_range[1])
        time.sleep(delay)
    
    def get_brand_statistics(self):
        """
        Scrape general statistics about the brand from Trustpilot.
        
        Returns:
            dict: Statistics including rating, review count, rating distribution
        """
        try:
            response = self.session.get(self.brand_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract brand statistics
            stats = {}
            
            # Overall TrustScore
            trustscore_element = soup.select_one('div[data-rating-distribution-panel-modal-trigger]')
            if trustscore_element:
                stats['trust_score'] = trustscore_element.get('data-rating-value', 'N/A')
            
            # Total reviews count
            reviews_count_element = soup.select_one('span[data-reviews-count-typography]')
            if reviews_count_element:
                stats['reviews_count'] = reviews_count_element.text.strip()
            
            # Rating distribution
            rating_distribution = {}
            distribution_elements = soup.select('div.styles_ratingDistribution__OlrLS div')
            
            for i, element in enumerate(distribution_elements):
                if i < 5:  # 5 stars rating distribution
                    star_count = 5 - i
                    percentage_element = element.select_one('div.styles_cell__qnPHy:nth-child(3)')
                    if percentage_element:
                        rating_distribution[f'{star_count}_star'] = percentage_element.text.strip()
            
            stats['rating_distribution'] = rating_distribution
            
            return stats
            
        except Exception as e:
            print(f"Error getting brand statistics: {e}")
            return {"error": str(e)}
    
    def get_reviews(self, page=1, limit=20):
        """
        Scrape customer reviews for the brand.
        
        Args:
            page (int): Page number to scrape
            limit (int): Maximum number of reviews to collect
            
        Returns:
            list: List of review dictionaries
        """
        reviews = []
        current_page = page
        
        while len(reviews) < limit:
            try:
                url = f"{self.brand_url}?page={current_page}"
                response = self.session.get(url)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                review_elements = soup.select('div.styles_reviewCard__hcAvl')
                
                if not review_elements:
                    break  # No more reviews found
                
                for review_element in review_elements:
                    if len(reviews) >= limit:
                        break
                    
                    review = {}
                    
                    # Rating
                    rating_element = review_element.select_one('div[data-service-review-rating]')
                    if rating_element:
                        review['rating'] = rating_element.get('data-service-review-rating', 'N/A')
                    
                    # Title
                    title_element = review_element.select_one('h2[data-review-title-typography]')
                    if title_element:
                        review['title'] = title_element.text.strip()
                    
                    # Content
                    content_element = review_element.select_one('p[data-service-review-text-typography]')
                    if content_element:
                        review['content'] = content_element.text.strip()
                    
                    # Author
                    author_element = review_element.select_one('span[data-consumer-name-typography]')
                    if author_element:
                        review['author'] = author_element.text.strip()
                    
                    # Date
                    date_element = review_element.select_one('time[datetime]')
                    if date_element:
                        review['date'] = date_element.get('datetime')
                    
                    reviews.append(review)
                
                # Add delay before next page
                self._random_delay()
                current_page += 1
                
            except Exception as e:
                print(f"Error getting reviews on page {current_page}: {e}")
                break
        
        return reviews[:limit]