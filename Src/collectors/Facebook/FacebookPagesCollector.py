import requests
import time
import json
from datetime import datetime
from typing import List, Dict, Optional, Union
from urllib.parse import urlparse


class FacebookPagesScraper:
    """
    Classe pour collecter les informations des pages Facebook
    via l'API Apify Facebook Pages Scraper
    """
    
    def __init__(self, apify_token: str):
        """
        Initialise le scraper avec le token Apify
        
        Args:
            apify_token (str): Token d'authentification Apify
        """
        self.apify_token = apify_token
        self.actor_id = 'apify~facebook-pages-scraper'
        self.base_url = 'https://api.apify.com/v2'
        
    def _normalize_facebook_url(self, url: str) -> str:
        """
        Normalise l'URL Facebook pour s'assurer qu'elle est au bon format
        
        Args:
            url (str): URL à normaliser
            
        Returns:
            str: URL normalisée
        """
        # Supprimer les espaces
        url = url.strip()
        
        # Ajouter le protocole si manquant
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        # Convertir en format web.facebook.com si nécessaire
        if 'facebook.com' in url and not url.startswith('https://web.facebook.com'):
            parsed = urlparse(url)
            path = parsed.path
            url = f'https://web.facebook.com{path}'
            
        return url
    
    def _prepare_start_urls(self, urls: List[str]) -> List[Dict]:
        """
        Prépare la liste des URLs au format requis par Apify
        
        Args:
            urls (List[str]): Liste des URLs à scraper
            
        Returns:
            List[Dict]: URLs formatées pour Apify
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
        Lance une nouvelle exécution du scraper
        
        Args:
            urls (List[str]): Liste des URLs des pages à scraper
            
        Returns:
            str: ID de l'exécution
            
        Raises:
            Exception: Si le lancement échoue
        """
        start_urls = self._prepare_start_urls(urls)
        actor_input = {"startUrls": start_urls}
        
        response = requests.post(
            f'{self.base_url}/acts/{self.actor_id}/runs?token={self.apify_token}',
            json=actor_input
        )
        
        if response.status_code != 201:
            raise Exception(f"Erreur au lancement du scraper: {response.status_code} - {response.text}")
            
        run_data = response.json()
        run_id = run_data.get('data', {}).get('id')
        
        if not run_id:
            raise Exception("Impossible de récupérer l'ID du run")
            
        return run_id
    
    def _wait_for_completion(self, run_id: str, timeout: int = 300, check_interval: int = 5) -> Dict:
        """
        Attend la fin de l'exécution du scraper
        
        Args:
            run_id (str): ID de l'exécution
            timeout (int): Timeout en secondes (défaut: 300s)
            check_interval (int): Intervalle de vérification en secondes (défaut: 5s)
            
        Returns:
            Dict: Données du statut final
            
        Raises:
            Exception: Si l'exécution échoue ou dépasse le timeout
        """
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout:
                raise Exception(f"Timeout dépassé ({timeout}s)")
                
            response = requests.get(
                f'{self.base_url}/actor-runs/{run_id}?token={self.apify_token}'
            )
            
            if response.status_code != 200:
                raise Exception(f"Erreur lors de la vérification du statut: {response.status_code}")
                
            run_status_data = response.json().get('data', {})
            status = run_status_data.get('status')
            
            print(f"🔄 Statut actuel: {status}")
            
            if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                break
                
            time.sleep(check_interval)
        
        if status != 'SUCCEEDED':
            raise Exception(f"Le scraper a échoué. Statut final: {status}")
            
        return run_status_data
    
    def _fetch_dataset(self, dataset_id: str) -> List[Dict]:
        """
        Récupère les données du dataset
        
        Args:
            dataset_id (str): ID du dataset
            
        Returns:
            List[Dict]: Données brutes du dataset
            
        Raises:
            Exception: Si la récupération échoue
        """
        response = requests.get(
            f'{self.base_url}/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
        )
        
        if response.status_code != 200:
            raise Exception(f"Erreur lors de la récupération des données: {response.status_code} - {response.text}")
            
        return response.json()
    
    def _safe_get_nested(self, data: Dict, *keys, default=None):
        """
        Récupère une valeur imbriquée de manière sécurisée
        
        Args:
            data (Dict): Dictionnaire source
            *keys: Clés imbriquées
            default: Valeur par défaut
            
        Returns:
            Valeur trouvée ou valeur par défaut
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
        
        Args:
            raw_page (Dict): Données brutes de la page
            
        Returns:
            Dict: Page formatée
        """
        # Extraction sécurisée des métriques
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
        Scrape les pages Facebook spécifiées
        
        Args:
            urls (Union[str, List[str]]): URL ou liste d'URLs des pages Facebook
            timeout (int): Timeout en secondes pour l'exécution
            save_to_file (Optional[str]): Chemin du fichier de sauvegarde (optionnel)
            
        Returns:
            List[Dict]: Liste des pages scrapées et formatées
            
        Raises:
            Exception: Si le scraping échoue
        """
        # Convertir en liste si c'est une chaîne
        if isinstance(urls, str):
            urls = [urls]
            
        print(f"🚀 Lancement du scraping pour {len(urls)} page(s) Facebook")
        for url in urls:
            print(f"   📄 {url}")
        
        # 1. Lancer le scraper
        run_id = self._start_scraping_run(urls)
        print(f"✅ Scraper lancé avec l'ID: {run_id}")
        
        # 2. Attendre la fin
        print("⏳ Attente de la fin de l'exécution...")
        run_status_data = self._wait_for_completion(run_id, timeout)
        
        # 3. Récupérer les données
        dataset_id = run_status_data.get('defaultDatasetId')
        if not dataset_id:
            raise Exception("Impossible de récupérer l'ID du dataset")
            
        print("📥 Récupération des données...")
        raw_data = self._fetch_dataset(dataset_id)
        
        # 4. Transformer les données
        pages = []
        for raw_page in raw_data:
            try:
                transformed_page = self._transform_page_data(raw_page)
                pages.append(transformed_page)
            except Exception as e:
                print(f"⚠️ Erreur lors de la transformation d'une page: {e}")
                continue
                
        print(f"✅ {len(pages)} page(s) traitée(s) avec succès")
        
        # 5. Sauvegarder si demandé
        if save_to_file:
            self.save_pages(pages, save_to_file)
            
        return pages
    
    def save_pages(self, pages: List[Dict], filename: str) -> None:
        """
        Sauvegarde les pages dans un fichier JSON
        
        Args:
            pages (List[Dict]): Liste des pages à sauvegarder
            filename (str): Nom du fichier de sauvegarde
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(pages, f, ensure_ascii=False, indent=2)
        print(f"💾 Pages sauvegardées dans '{filename}'")
    
    def get_pages_summary(self, pages: List[Dict]) -> None:
        """
        Affiche un résumé des pages scrapées
        
        Args:
            pages (List[Dict]): Liste des pages
        """
        print("\n📊 RÉSUMÉ DES PAGES FACEBOOK:")
        print("=" * 60)
        
        for page in pages:
            print(f"📄 {page['profile_name'] or page['page_name'] or 'Page sans nom'}")
            print(f"   🆔 ID: {page['profile_id']}")
            print(f"   🔗 URL: {page['url']}")
            print(f"   👥 Followers: {page['metrics']['followers']:,}")
            print(f"   👍 Likes: {page['metrics']['likes']:,}")
            print(f"   🏢 Compte business: {'Oui' if page['is_business_account'] else 'Non'}")
            if page['biography']:
                bio_preview = page['biography'][:100] + "..." if len(page['biography']) > 100 else page['biography']
                print(f"   📝 Description: {bio_preview}")
            print("-" * 60)
    
    def scrape_telecom_pages(self, save_to_file: Optional[str] = None) -> List[Dict]:
        """
        Méthode de convenance pour scraper les pages des opérateurs télécoms marocains
        
        Args:
            save_to_file (Optional[str]): Fichier de sauvegarde
            
        Returns:
            List[Dict]: Pages des opérateurs télécoms
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
    # Initialisation
    APIFY_TOKEN = 'apify_api_YrPT59TKw8olQkGrrCIpP8pgf6lbNs2KWpnv'
    scraper = FacebookPagesScraper(APIFY_TOKEN)
    
    try:
        # Scraper plusieurs pages
        pages = scraper.scrape_pages([
            "https://web.facebook.com/orangemaroc",
            "https://web.facebook.com/inwi.ma",
            "https://web.facebook.com/maroctelecom",
            "https://web.facebook.com/yoxobyOrange"
        ], save_to_file="telecom_facebook_pages.json")
        
        # Afficher le résumé
        scraper.get_pages_summary(pages)
        
        # Utiliser la méthode de convenance pour les télécoms
        # telecom_pages = scraper.scrape_telecom_pages("telecom_pages.json")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")