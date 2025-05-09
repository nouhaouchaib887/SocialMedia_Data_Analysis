import requests
from bs4 import BeautifulSoup
import time
import json
import os
import random
from datetime import datetime


class TrustpilotCollector:
    def __init__(self, brand_name, output_dir="data", delay_range=(1, 3),  language="fr"):
        """
        Initialize the Trustpilot scraper.
        
        Args:
            brand_name (str): Name of the brand to scrape on Trustpilot
            output_dir (str): Directory to save output data
            delay_range (tuple): Range of seconds to delay between requests (min, max)
        """
        self.brand_name = brand_name
        self.language= language
        self.brand_url = f"https://{self.language}.trustpilot.com/review/www.{brand_name}.ma"
        self.output_dir = output_dir
        self.delay_range = delay_range
        # Create data directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Setup session with headers to mimic browser
        self.session = requests.Session()
        # Define language preferences in the headers
        language_header = f"{self.language}-{self.language.upper()},{self.language};q=0.9"
        if self.language != "en":
            language_header += ",en-US;q=0.8,en;q=0.7"
    
        # Update the session headers with the user-agent, language preferences, and other details
        self.session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': language_header,
    'Referer': f"https://{self.language}.trustpilot.com/",
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
        })

        # Add a cookie for the language preference
        self.session.cookies.set("tp-language", self.language)
        
    
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
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
            # Extract brand statistics
            stats = {}
        
            # Overall TrustScore - Updated selector to find the star rating in the alt attribute
            trustscore_element = soup.select_one('div.star-rating_starRating__sdbkn img')
            if trustscore_element and trustscore_element.get('alt'):
                alt_text = trustscore_element.get('alt')
                # Extract numeric value from text like "TrustScore 1,5 sur 5"
                try:
                    # Handle both comma and dot decimal separators
                    score_text = alt_text.split(' ')[1].replace(',', '.')
                    stats['trust_score'] = float(score_text)
                except (IndexError, ValueError):
                    stats['trust_score'] = alt_text
        
            # Total reviews count - Updated selector
            reviews_count_element = soup.select_one('p.styles_reviewCount__NXlel')
            if reviews_count_element:
                # Extract numeric value from text like "486 avis"
                reviews_text = reviews_count_element.text.strip()
                try:
                    stats['reviews_count'] = int(reviews_text.split(' ')[0])
                except (IndexError, ValueError):
                    stats['reviews_count'] = reviews_text
        
            # Rating distribution
            rating_distribution = {}
            distribution_elements = soup.select('div.rating-distribution-row_row__TH3OE')
            for element in distribution_elements:
                star = element.get('data-star-rating')
                bar_span = element.select_one('span.rating-distribution-row_barValue__iFje4')
                if star and bar_span:
                    # Convert 'five' → '5', 'four' → '4', etc.
                    star_mapping = {
                    'five': '5',
                    'four': '4',
                    'three': '3',
                    'two': '2',
                    'one': '1',
                    }
                    star_count = star_mapping.get(star, star)
                    style = bar_span.get('style', '')
                    # Extract width percentage from style and convert to float
                    try:
                        percentage_text = style.split('width:')[-1].split('%')[0].strip()
                        percentage_value = round(float(percentage_text),2)
                        rating_distribution[f'{star_count}'] = percentage_value
                    except (IndexError, ValueError):
                        rating_distribution[f'{star_count}'] = style
        
            stats['rating_distribution'] = rating_distribution
            return stats
        except Exception as e:
            print(f"Error occurred: {e}")
            return {}
    
    def get_reviews(self, pages, min_date=None):
        """
        Scrape customer reviews for the brand, optionally filtering by minimum date.
    
        Args:
        pages (int): Number of pages to scrape
        min_date (str|datetime): Minimum date for reviews (format: 'YYYY-MM-DD' or datetime object)
        
        Returns:
        list: List of review dictionaries
        """
        reviews = []
    
        # Convert min_date to datetime object if it's a string
        if min_date and isinstance(min_date, str):
            from datetime import datetime
            try:
                min_date = datetime.strptime(min_date, '%Y-%m-%d').date()
            except ValueError:
                print("Invalid date format. Please use 'YYYY-MM-DD' format.")
                return reviews
    
        for i in range(1, pages + 1):
            try:
                url = f"{self.brand_url}?page={i}"
                response = self.session.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                review_containers = soup.select('article[class^="paper_paper__"]')

                for container in review_containers:
                    # Extract review date
                    date_experience_tag = container.select_one('p[data-service-review-date-of-experience-typography="true"]')
                    if not date_experience_tag:
                        continue
                    
                    date_experience_text = date_experience_tag.text.strip()
                    date_str = date_experience_text.split(": ")[-1].strip()
                
                    # Parse date with multiple format support
                    from datetime import datetime
                    
                    # Liste des formats possibles
                    date_formats = [
                        '%d %B %Y',       # Format français: 25 mars 2023
                        '%B %d, %Y',      # Format anglais: July 17, 2024
                        '%d/%m/%Y',       # Format numérique avec /
                        '%Y-%m-%d',       # Format ISO
                        '%d-%m-%Y',       # Format avec tirets
                        '%b %d, %Y'       # Format court: Jul 17, 2024
                    ]
                    
                    # Traduction des mois (français -> anglais)
                    month_translation = {
                        'janvier': 'January', 'février': 'February', 'mars': 'March',
                        'avril': 'April', 'mai': 'May', 'juin': 'June',
                        'juillet': 'July', 'août': 'August', 'septembre': 'September',
                        'octobre': 'October', 'novembre': 'November', 'décembre': 'December'
                    }
                    
                    # Normaliser la date en anglais
                    for fr, en in month_translation.items():
                        date_str = date_str.replace(fr, en)
                    
                    # Essayer chaque format jusqu'à trouver le bon
                    review_date = None
                    for date_format in date_formats:
                        try:
                            review_date = datetime.strptime(date_str, date_format).date()
                            break  # Sortir de la boucle si le parsing a réussi
                        except ValueError:
                            continue
                            
                    # Si aucun format n'a fonctionné
                    if not review_date:
                        print(f"Impossible de parser la date '{date_str}' avec les formats disponibles")
                        continue
                    
                    # Skip if review is before min_date
                    if min_date and review_date < min_date:
                        continue
                
                    # Extract other review data
                    rating_tag = container.select_one('[data-service-review-rating]')
                    rating = int(rating_tag['data-service-review-rating']) if rating_tag else None
                
                    review_content_tag = container.select_one('section[class^="styles_reviewContentwrapper"] p')
                    review_content = review_content_tag.text.strip() if review_content_tag else "Non trouvé"

                    reviews.append({
                        'brand': self.brand_name,
                        'Rating': rating,
                        'Review': review_content,
                        'Parsed Date': str(review_date)  # Convertir en string pour JSON
                     })
                
                self._random_delay()
                
            except Exception as e:
                print(f"Error getting reviews on page {i}: {e}")
                continue
    
        return reviews