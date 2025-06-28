import requests
import time
import json
from typing import List, Dict, Optional
from urllib.parse import urlparse


class ApifyAPIClient:
    """
    Client pour interagir avec l'API Apify Facebook Pages Scraper
    """
    
    def __init__(self, apify_token: str):
        """
        Initialise le client API avec le token Apify
        """
        if not apify_token:
            raise ValueError("Apify token cannot be empty.")
            
        self.apify_token = apify_token
        self.actor_id = 'apify~facebook-pages-scraper'
        self.base_url = 'https://api.apify.com/v2'
    
    def _normalize_facebook_url(self, url: str) -> str:
        """
        Normalise l'URL Facebook pour s'assurer qu'elle est au bon format
        """
        url = url.strip()

        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        if 'facebook.com' in url and not url.startswith('https://web.facebook.com'):
            try:
                parsed = urlparse(url)
                # handle domain like m.facebook.com
                domain = parsed.netloc
                # Keep path including leading slash
                path = parsed.path if parsed.path else '/'
                # Add query and fragment if they exist
                query = f'?{parsed.query}' if parsed.query else ''
                fragment = f'#{parsed.fragment}' if parsed.fragment else ''

                # Reconstruct the URL with web.facebook.com and keep the rest
                url = f'https://web.facebook.com{path}{query}{fragment}'
            except Exception as e:
                # In a real scenario, you might log this error or handle it differently
                print(f"⚠️ Erreur lors de l'analyse de l'URL {url}: {e}")
                # Return original URL or handle as invalid
                return url # Or raise ValueError or return None
        return url
    
    def _prepare_start_urls(self, urls: List[str]) -> List[Dict]:
        """
        Prépare la liste des URLs au format requis par Apify
        """
        start_urls = []
        for url in urls:
            normalized_url = self._normalize_facebook_url(url)
            start_urls.append({
                "url": normalized_url,
                "method": "GET"
            })
        return start_urls
    
    def start_scraping_run(self, urls: List[str]) -> str:
        """
        Lance une nouvelle exécution du scraper sur Apify
        """
        if not urls:
            raise ValueError("Aucune URL fournie pour le scraping.")

        start_urls = self._prepare_start_urls(urls)
        if not start_urls:
         raise ValueError("Aucune URL valide n'a été préparée pour le scraping.")

  
        actor_input = {"startUrls": start_urls}
        
        try:
            response = requests.post(
                f'{self.base_url}/acts/{self.actor_id}/runs?token={self.apify_token}',
                json=actor_input
            )
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erreur de requête API lors du lancement du scraper: {e}")
            
        run_data = response.json()
        run_id = run_data.get('data', {}).get('id')
        
        if not run_id:
            raise Exception("Impossible de récupérer l'ID du run depuis la réponse API.")
            
        return run_id
    
    def get_run_status(self, run_id: str) -> Dict:
        """
        Récupère le statut d'une exécution
        """
        try:
            response = requests.get(
                f'{self.base_url}/actor-runs/{run_id}?token={self.apify_token}'
            )
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erreur lors de la vérification du statut de l'exécution {run_id}: {e}")
            
        return response.json().get('data', {})
    
    def wait_for_completion(self, run_id: str, timeout: int = 300, check_interval: int = 5) -> Dict:
        """
        Attend la fin de l'exécution du scraper
        """
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout dépassé ({timeout}s) en attente de l'exécution {run_id}.")
                
            try:
                run_status_data = self.get_run_status(run_id)
                status = run_status_data.get('status')
                
                print(f"🔄 Statut actuel de l'exécution {run_id}: {status}")
                
                if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                    break
                    
            except Exception as e:
                print(f"⚠️ Erreur lors de la vérification du statut: {e}")
                # Continuer d'essayer après une erreur temporaire
                time.sleep(check_interval)
                continue
                
            time.sleep(check_interval)
        
        if status != 'SUCCEEDED':
            raise RuntimeError(f"Le scraper a échoué ou a été interrompu. Statut final: {status}")
            
        return run_status_data
    
    def fetch_dataset(self, dataset_id: str) -> List[Dict]:
        """
        Récupère les données du dataset de l'exécution
        """
        if not dataset_id:
            return []

        try:
            response = requests.get(
                f'{self.base_url}/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
            )
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erreur de requête API lors de la récupération du dataset {dataset_id}: {e}")
            
        try:
             return response.json()
        except json.JSONDecodeError:
             print(f"⚠️ Avertissement: La réponse de l'API pour le dataset {dataset_id} n'est pas du JSON valide.")
             return []