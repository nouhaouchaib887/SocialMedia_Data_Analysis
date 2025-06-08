import requests
import time
import json
from datetime import datetime
from typing import List, Dict, Optional, Union


class InstagramProfileScraper:
    """
    Classe pour collecter les informations de base des profils Instagram
    via l'API Apify Instagram Profile Scraper
    """
    
    def __init__(self, apify_token: str):
        """
        Initialise le scraper avec le token Apify
        
        Args:
            apify_token (str): Token d'authentification Apify
        """
        self.apify_token = apify_token
        self.actor_id = 'apify~instagram-profile-scraper'
        self.base_url = 'https://api.apify.com/v2'
        
    def _start_scraping_run(self, usernames: List[str]) -> str:
        """
        Lance une nouvelle exécution du scraper
        
        Args:
            usernames (List[str]): Liste des noms d'utilisateur à scraper
            
        Returns:
            str: ID de l'exécution
            
        Raises:
            Exception: Si le lancement échoue
        """
        actor_input = {"usernames": usernames}
        
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
    
    def _transform_profile_data(self, raw_profile: Dict) -> Dict:
        """
        Transforme les données brutes du profil en format standardisé
        
        Args:
            raw_profile (Dict): Données brutes du profil
            
        Returns:
            Dict: Profil formaté
        """
        return {
            "platform": "instagram",
            "profile_id": raw_profile.get("id"),
            "profile_name": raw_profile.get("fullName"),
            "user_name": raw_profile.get("username"),
            "url": raw_profile.get("url"),
            "biography": raw_profile.get("biography"),
            "metrics": {
                "likes": None,
                "followers": raw_profile.get("followersCount", 0),
                "following": raw_profile.get("followingCount", 0),
                "posts": raw_profile.get("mediaCount", 0)
            },
            "is_private": raw_profile.get("private", False),
            "is_verified": raw_profile.get("verified", False),
            "is_business_account": raw_profile.get("isBusinessAccount", False),
            "profile_pic_url": raw_profile.get("profilePicUrl"),
            "external_url": raw_profile.get("externalUrl"),
            "scraped_at": datetime.now().isoformat()
        }
    
    def scrape_profiles(self, usernames: Union[str, List[str]], 
                       timeout: int = 300, 
                       save_to_file: Optional[str] = None) -> List[Dict]:
        """
        Scrape les profils Instagram spécifiés
        
        Args:
            usernames (Union[str, List[str]]): Nom d'utilisateur ou liste de noms d'utilisateur
            timeout (int): Timeout en secondes pour l'exécution
            save_to_file (Optional[str]): Chemin du fichier de sauvegarde (optionnel)
            
        Returns:
            List[Dict]: Liste des profils scrapés et formatés
            
        Raises:
            Exception: Si le scraping échoue
        """
        # Convertir en liste si c'est une chaîne
        if isinstance(usernames, str):
            usernames = [usernames]
            
        print(f"🚀 Lancement du scraping pour: {', '.join(usernames)}")
        
        # 1. Lancer le scraper
        run_id = self._start_scraping_run(usernames)
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
        profiles = []
        for raw_profile in raw_data:
            transformed_profile = self._transform_profile_data(raw_profile)
            profiles.append(transformed_profile)
            
        print(f"✅ {len(profiles)} profil(s) traité(s) avec succès")
        
        # 5. Sauvegarder si demandé
        if save_to_file:
            self.save_profiles(profiles, save_to_file)
            
        return profiles
    
    def save_profiles(self, profiles: List[Dict], filename: str) -> None:
        """
        Sauvegarde les profils dans un fichier JSON
        
        Args:
            profiles (List[Dict]): Liste des profils à sauvegarder
            filename (str): Nom du fichier de sauvegarde
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(profiles, f, ensure_ascii=False, indent=2)
        print(f"💾 Profils sauvegardés dans '{filename}'")
    
    def get_profile_summary(self, profiles: List[Dict]) -> None:
        """
        Affiche un résumé des profils scrapés
        
        Args:
            profiles (List[Dict]): Liste des profils
        """
        print("\n📊 RÉSUMÉ DES PROFILS:")
        print("=" * 50)
        
        for profile in profiles:
            print(f"👤 {profile['user_name']} (@{profile['profile_name'] or 'N/A'})")
            print(f"   📍 URL: {profile['url']}")
            print(f"   👥 Followers: {profile['metrics']['followers']:,}")
            print(f"   📸 Posts: {profile['metrics']['posts']:,}")
            print(f"   🔒 Privé: {'Oui' if profile['is_private'] else 'Non'}")
            print(f"   ✅ Vérifié: {'Oui' if profile['is_verified'] else 'Non'}")
            print(f"   🏢 Compte business: {'Oui' if profile['is_business_account'] else 'Non'}")
            if profile['biography']:
                bio_preview = profile['biography'][:100] + "..." if len(profile['biography']) > 100 else profile['biography']
                print(f"   📝 Biographie: {bio_preview}")
            print("-" * 50)


# Exemple d'utilisation
if __name__ == "__main__":
    # Initialisation
    APIFY_TOKEN = 'apify_api_YrPT59TKw8olQkGrrCIpP8pgf6lbNs2KWpnv'
    scraper = InstagramProfileScraper(APIFY_TOKEN)
    
    try:
        # Scraper un seul profil
        profiles = scraper.scrape_profiles(
            usernames=["orangemaroc","inwi_maroc","maroctelecom"],
            save_to_file="Instagram_profiles.json"
        )
        
        # Afficher le résumé
        scraper.get_profile_summary(profiles)
        
        # Scraper plusieurs profils
        # profiles = scraper.scrape_profiles(
        #     usernames=["orangemaroc", "maroctelecom", "inwi"],
        #     save_to_file="telecom_profiles.json"
        # )
        
    except Exception as e:
        print(f"❌ Erreur: {e}")