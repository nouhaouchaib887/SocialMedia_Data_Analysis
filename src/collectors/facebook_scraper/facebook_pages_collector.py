import requests
import time
import json
from datetime import datetime
from typing import List, Dict, Optional, Union
from urllib.parse import urlparse
import os # Import os for file operations error handling

class FacebookPagesScraper:
    """
    Classe pour collecter les informations des pages Facebook
    via l'API Apify Facebook Pages Scraper
    """
    
    def __init__(self, apify_token: str):
        """
        Initialise le scraper avec le token Apify
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
                # Ensure path is not empty, handle root url case
                path = parsed.path if parsed.path else '/'
                url = f'https://web.facebook.com{path}'
            except Exception as e:
                print(f"⚠️ Erreur lors de l'analyse de l'URL {url}: {e}")
                # Return original url if parsing fails
                return url
            
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
    
    def _start_scraping_run(self, urls: List[str]) -> str:
        """
        Lance une nouvelle exécution du scraper sur Apify
        """
        if not urls:
            raise ValueError("Aucune URL fournie pour le scraping.")

        start_urls = self._prepare_start_urls(urls)
        actor_input = {"startUrls": start_urls}
        
        try:
            response = requests.post(
                f'{self.base_url}/acts/{self.actor_id}/runs?token={self.apify_token}',
                json=actor_input
            )
            response.raise_for_status() # Lève une exception pour les codes d'erreur HTTP (4xx ou 5xx)
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erreur de requête API lors du lancement du scraper: {e}")
            
        run_data = response.json()
        run_id = run_data.get('data', {}).get('id')
        
        if not run_id:
            raise Exception("Impossible de récupérer l'ID du run depuis la réponse API.")
            
        return run_id
    
    def _wait_for_completion(self, run_id: str, timeout: int = 300, check_interval: int = 5) -> Dict:
        """
        Attend la fin de l'exécution du scraper
        """
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout dépassé ({timeout}s) en attente de l'exécution {run_id}.")
                
            try:
                response = requests.get(
                    f'{self.base_url}/actor-runs/{run_id}?token={self.apify_token}'
                )
                response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Erreur lors de la vérification du statut de l'exécution {run_id}: {e}")
                # Continuer d'essayer après une erreur temporaire de requête
                time.sleep(check_interval)
                continue
                
            run_status_data = response.json().get('data', {})
            status = run_status_data.get('status')
            
            print(f"🔄 Statut actuel de l'exécution {run_id}: {status}")
            
            if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                break
                
            time.sleep(check_interval)
        
        if status != 'SUCCEEDED':
            raise RuntimeError(f"Le scraper a échoué ou a été interrompu. Statut final: {status}")
            
        return run_status_data
    
    def _fetch_dataset(self, dataset_id: str) -> List[Dict]:
        """
        Récupère les données du dataset de l'exécution
        """
        if not dataset_id:
            return [] # Aucun dataset à récupérer

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
             return [] # Retourne une liste vide si la réponse n'est pas JSON
    
    def _safe_get_nested(self, data: Dict, *keys, default=None):
        """
        Récupère une valeur imbriquée de manière sécurisée
        """
        try:
            for key in keys:
                data = data[key]
            return data
        except (KeyError, TypeError, AttributeError):
            return default
    
    def _transform_page_data(self, raw_page: Dict) -> Dict:
        """
        Transforme les données brutes de la page en format standardisé
        """
        page_ad_library = raw_page.get('pageAdLibrary', {})
        is_business_active = self._safe_get_nested(page_ad_library, 'is_business_page_active', False)
        
        return {
            "platform": "facebook",
            "profile_id": raw_page.get("pageId"),
            "profile_name": raw_page.get("title"),
            "page_name": raw_page.get("pageName"),
            "url": raw_page.get("pageUrl"),
            "creation_date": raw_page.get("creation_date"),
            "biography": raw_page.get("intro"),
            "metrics": {
                "likes": raw_page.get("likes", 0),
                "followers": raw_page.get("followers", 0),
            },
            "is_business_account": is_business_active,
            "scraped_at": datetime.now().isoformat()
        }
    
    def scrape_pages(self, urls: Union[str, List[str]], 
                    timeout: int = 300, 
                    save_to_file: Optional[str] = None) -> List[Dict]:
        """
        Lance le scraping des pages Facebook spécifiées via Apify et retourne les données formatées.
        """
        if isinstance(urls, str):
            urls = [urls]
            
        if not urls:
            print("Aucune URL fournie. Fin du processus.")
            return []

        print(f"🚀 Lancement du scraping pour {len(urls)} page(s) Facebook...")
        for url in urls:
            print(f"   📄 {url}")
        
        try:
            # 1. Lancer le scraper
            run_id = self._start_scraping_run(urls)
            print(f"✅ Scraper lancé avec l'ID: {run_id}")
            
            # 2. Attendre la fin
            print("⏳ Attente de la fin de l'exécution...")
            run_status_data = self._wait_for_completion(run_id, timeout)
            print("✅ Exécution terminée.")
            
            # 3. Récupérer les données
            dataset_id = run_status_data.get('defaultDatasetId')
            if not dataset_id:
                print("⚠️ Aucun dataset produit par cette exécution.")
                raw_data = []
            else:
                print(f"📥 Récupération des données depuis le dataset {dataset_id}...")
                raw_data = self._fetch_dataset(dataset_id)
                print(f"✅ {len(raw_data)} enregistrement(s) brut(s) récupéré(s).")
            
            # 4. Transformer les données
            pages = []
            for raw_page in raw_data:
                try:
                    transformed_page = self._transform_page_data(raw_page)
                    pages.append(transformed_page)
                except Exception as e:
                    print(f"⚠️ Erreur lors de la transformation d'une page: {e}. Page brute: {raw_page.get('pageUrl', 'N/A')}")
                    continue
                    
            print(f"✅ {len(pages)} page(s) formatée(s) avec succès")
            
            # 5. Sauvegarder si demandé
            if save_to_file:
                self.save_pages(pages, save_to_file)
                
            return pages

        except (ValueError, RuntimeError, TimeoutError, Exception) as e:
            print(f"❌ Échec du scraping: {e}")
            return [] # Retourne une liste vide en cas d'échec global
    
    def save_pages(self, pages: List[Dict], filename: str) -> None:
        """
        Sauvegarde les pages dans un fichier JSON
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(pages, f, ensure_ascii=False, indent=2)
            print(f"💾 Pages sauvegardées dans '{filename}'")
        except IOError as e:
            print(f"❌ Erreur lors de la sauvegarde du fichier '{filename}': {e}")
        except Exception as e:
            print(f"❌ Une erreur inattendue est survenue lors de la sauvegarde: {e}")

    def get_pages_summary(self, pages: List[Dict]) -> None:
        """
        Affiche un résumé des pages scrapées
        """
        if not pages:
            print("\n📊 Aucun résumé disponible (aucune page scrapée).")
            return

        print("\n📊 RÉSUMÉ DES PAGES FACEBOOK:")
        print("=" * 60)
        
        for page in pages:
            name = page.get('profile_name') or page.get('page_name') or 'Page sans nom'
            print(f"📄 {name}")
            print(f"   🆔 ID: {page.get('profile_id', 'N/A')}")
            print(f"   🔗 URL: {page.get('url', 'N/A')}")
            metrics = page.get('metrics', {})
            print(f"   👥 Followers: {metrics.get('followers', 0):,}")
            print(f"   👍 Likes: {metrics.get('likes', 0):,}")
            print(f"   🏢 Compte business: {'Oui' if page.get('is_business_account') else 'Non'}")
            biography = page.get('biography')
            if biography:
                bio_preview = biography[:100] + "..." if len(biography) > 100 else biography
                print(f"   📝 Description: {bio_preview}")
            print("-" * 60)
    
    def scrape_telecom_pages(self, save_to_file: Optional[str] = None) -> List[Dict]:
        """
        Méthode de convenance pour scraper les pages des opérateurs télécoms marocains
        """
        telecom_urls = [
            "https://web.facebook.com/orangemaroc",
            "https://web.facebook.com/inwi.ma", 
            "https://web.facebook.com/maroctelecom",
            "https://web.facebook.com/yoxobyOrange"
        ]
        
        return self.scrape_pages(telecom_urls, save_to_file=save_to_file)


# Exemple d'utilisation
if __name__ == "__main__":
   
    APIFY_TOKEN = '' 
    
    if APIFY_TOKEN == 'YOUR_APIFY_TOKEN':
        print("Veuillez remplacer 'YOUR_APIFY_TOKEN' par votre token Apify.")
    else:
        try:
            scraper = FacebookPagesScraper(APIFY_TOKEN)
            
            # Scraper plusieurs pages
            pages = scraper.scrape_pages([
                "https://web.facebook.com/orangemaroc",
                "https://web.facebook.com/inwi.ma",
                "https://web.facebook.com/maroctelecom",
                "https://web.facebook.com/yoxobyOrange"
            ], timeout=600, # Augmentez le timeout si nécessaire
               save_to_file="telecom_facebook_pages.json")
            
            # Afficher le résumé
            scraper.get_pages_summary(pages)
            
            
            
        except ValueError as e:
             print(f"❌ Erreur de configuration: {e}")
        except Exception as e:
            # Cette exception attrape les erreurs non gérées spécifiquement plus bas
            print(f"❌ Une erreur inattendue est survenue: {e}")