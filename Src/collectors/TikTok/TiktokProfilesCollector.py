import requests
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union


class TikTokProfileScraper:
    """
    Classe pour collecter les informations des profils TikTok
    via l'API Apify TikTok Profile Scraper
    """
    
    def __init__(self, apify_token: str):
        """
        Initialise le scraper avec le token Apify
        
        Args:
            apify_token (str): Token d'authentification Apify
        """
        self.apify_token = apify_token
        self.actor_id = 'clockworks~tiktok-profile-scraper'
        self.base_url = 'https://api.apify.com/v2'
        
    def _prepare_actor_input(self, 
                           profiles: List[str], 
                           results_per_page: int = 1) -> Dict:
        """
        Prépare les paramètres d'entrée pour l'actor Apify
        
        Args:
            profiles (List[str]): Liste des noms d'utilisateur TikTok
            results_per_page (int): Nombre de résultats par page
            
        Returns:
            Dict: Paramètres formatés pour l'actor
        """  
        actor_input = {
            "profiles": profiles,
            "resultsPerPage": results_per_page
        }
        
        return actor_input
    
    def _start_scraping_run(self, actor_input: Dict) -> str:
        """
        Lance une nouvelle exécution du scraper
        
        Args:
            actor_input (Dict): Paramètres d'entrée pour l'actor
            
        Returns:
            str: ID de l'exécution
            
        Raises:
            Exception: Si le lancement échoue
        """
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
    
    def _transform_profile_data(self, raw_profile: Dict) -> Dict:
        """
        Transforme les données brutes du profil en format standardisé
        
        Args:
            raw_profile (Dict): Données brutes du profil
            
        Returns:
            Dict: Profil formaté
        """
        author_meta = raw_profile.get("authorMeta", {})
        
        return {
            "platform": "tiktok",
            "profile_id": self._safe_get_nested(author_meta, "id"),
            "profile_name": self._safe_get_nested(author_meta, "nickName"),
            "page_name": self._safe_get_nested(author_meta, "name"),
            "url": self._safe_get_nested(author_meta, "profileUrl"),
            "biography": self._safe_get_nested(author_meta, "signature"),
            "metrics": {
                "likes": self._safe_get_nested(author_meta, "heart", 0),
                "followers": self._safe_get_nested(author_meta, "fans", 0),
            },
            "is_verified": self._safe_get_nested(author_meta, "verified", False),
            "scraped_at": datetime.now().isoformat()
        }
    
    def scrape_profiles(self, 
                       profiles: Union[str, List[str]], 
                       results_per_page: int = 1,
                       timeout: int = 300, 
                       save_to_file: Optional[str] = None) -> List[Dict]:
        """
        Scrape les profils TikTok spécifiés
        
        Args:
            profiles (Union[str, List[str]]): Nom d'utilisateur ou liste de noms d'utilisateur
            results_per_page (int): Nombre de résultats par page
            timeout (int): Timeout en secondes pour l'exécution
            save_to_file (Optional[str]): Chemin du fichier de sauvegarde (optionnel)
            
        Returns:
            List[Dict]: Liste des profils scrapés et formatés
            
        Raises:
            Exception: Si le scraping échoue
        """
        # Convertir en liste si c'est une chaîne
        if isinstance(profiles, str):
            profiles = [profiles]
            
        print(f"🚀 Lancement du scraping TikTok pour: {', '.join(profiles)}")
        
        # 1. Préparer les paramètres
        actor_input = self._prepare_actor_input(
            profiles=profiles,
            results_per_page=results_per_page
        )
        
        # 2. Lancer le scraper
        run_id = self._start_scraping_run(actor_input)
        print(f"✅ Scraper lancé avec l'ID: {run_id}")
        
        # 3. Attendre la fin
        print("⏳ Attente de la fin de l'exécution...")
        run_status_data = self._wait_for_completion(run_id, timeout)
        
        # 4. Récupérer les données
        dataset_id = run_status_data.get('defaultDatasetId')
        if not dataset_id:
            raise Exception("Impossible de récupérer l'ID du dataset")
            
        print("📥 Récupération des données...")
        raw_data = self._fetch_dataset(dataset_id)
        
        # 5. Transformer les données
        tiktok_profiles = []
        for raw_profile in raw_data:
            try:
                transformed_profile = self._transform_profile_data(raw_profile)
                tiktok_profiles.append(transformed_profile)
            except Exception as e:
                print(f"⚠️ Erreur lors de la transformation d'un profil: {e}")
                continue
                
        print(f"✅ {len(tiktok_profiles)} profil(s) TikTok traité(s) avec succès")
        
        # 6. Sauvegarder si demandé
        if save_to_file:
            self.save_profiles(tiktok_profiles, save_to_file)
            
        return tiktok_profiles
    
    def save_profiles(self, profiles: List[Dict], filename: str) -> None:
        """
        Sauvegarde les profils dans un fichier JSON
        
        Args:
            profiles (List[Dict]): Liste des profils à sauvegarder
            filename (str): Nom du fichier de sauvegarde
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(profiles, f, ensure_ascii=False, indent=2)
        print(f"💾 Profils TikTok sauvegardés dans '{filename}'")
    
    def get_profiles_summary(self, profiles: List[Dict]) -> None:
        """
        Affiche un résumé des profils TikTok scrapés
        
        Args:
            profiles (List[Dict]): Liste des profils
        """
        print("\n📊 RÉSUMÉ DES PROFILS TIKTOK:")
        print("=" * 60)
        
        for profile in profiles:
            if profile['profile_name']:
                print(f"   📝 Nom d'affichage: {profile['profile_name']}")
            print(f"   🆔 ID: {profile['profile_id']}")
            print(f"   🔗 URL: {profile['url']}")
            print(f"   👥 Followers: {profile['metrics']['followers']:,}")
            print(f"   💖 Likes totaux: {profile['metrics']['likes']:,}")
            print(f"   ✅ Vérifié: {'Oui' if profile['is_verified'] else 'Non'}")
            if profile['biography']:
                bio_preview = profile['biography'][:80] + "..." if len(profile['biography']) > 80 else profile['biography']
                print(f"   📄 Bio: {bio_preview}")
            print("-" * 60)
    
    def scrape_telecom_profiles(self, 
                              oldest_post_date: Optional[str] = None,
                              save_to_file: Optional[str] = None) -> List[Dict]:
        """
        Méthode de convenance pour scraper les profils TikTok des opérateurs télécoms marocains
        
        Args:
            oldest_post_date (Optional[str]): Date la plus ancienne (YYYY-MM-DD)
            save_to_file (Optional[str]): Fichier de sauvegarde
            
        Returns:
            List[Dict]: Profils des opérateurs télécoms
        """
        telecom_profiles = [
            "orangemaroc",
            "inwi.maroc", 
            "maroctelecom"
        ]
        
        return self.scrape_profiles(
            profiles=telecom_profiles,
            oldest_post_date=oldest_post_date,
            save_to_file=save_to_file
        )


# Exemple d'utilisation
if __name__ == "__main__":
    # Initialisation
    APIFY_TOKEN = 'apify_api_YrPT59TKw8olQkGrrCIpP8pgf6lbNs2KWpnv'
    scraper = TikTokProfileScraper(APIFY_TOKEN)
    
    try:
        # Scraper un seul profil
        profiles = scraper.scrape_profiles(
            "orangemaroc",
            save_to_file="orange_tiktok_profile.json"
        )
        
        # Scraper plusieurs profils avec options personnalisées
        profiles = scraper.scrape_profiles(
            profiles=["orangemaroc", "inwi.maroc", "maroctelecom"],
            results_per_page=5,
            save_to_file="telecom_tiktok_profiles.json"
        )
        
        # Afficher le résumé
        scraper.get_profiles_summary(profiles)
        
        # Utiliser la méthode de convenance pour les télécoms
        # telecom_profiles = scraper.scrape_telecom_profiles(
        #     oldest_post_date="2025-06-01",
        #     save_to_file="telecom_tiktok.json"
        # )
        
    except Exception as e:
        print(f"❌ Erreur: {e}")