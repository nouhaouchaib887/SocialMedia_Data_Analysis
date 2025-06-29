import requests
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union


import requests
import time
import json
import os # Import os for checking file paths (optional but good practice)
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union

# Define a custom exception for scraper errors
class TikTokScraperError(Exception):
    """Custom exception for TikTok Scraper errors."""
    pass

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
            
        Raises:
            TikTokScraperError: Si le token n'est pas fourni.
        """
        if not apify_token:
             raise TikTokScraperError("Apify token is required.")
             
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
        # Basic input validation
        if not isinstance(profiles, list) or not profiles:
             raise TikTokScraperError("Profiles must be a non-empty list of strings.")
        if not all(isinstance(p, str) for p in profiles):
             raise TikTokScraperError("All profile names must be strings.")
        if not isinstance(results_per_page, int) or results_per_page <= 0:
             print("⚠️ Warning: results_per_page should be a positive integer. Defaulting to 1.")
             results_per_page = 1 # Or raise an error depending on desired strictness

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
            TikTokScraperError: Si le lancement échoue (network error, API error, invalid response)
        """
        print(f"🚀 Lancement de l'actor {self.actor_id}...")
        url = f'{self.base_url}/acts/{self.actor_id}/runs?token={self.apify_token}'
        try:
            response = requests.post(url, json=actor_input)
            response.raise_for_status() # Lève une exception pour les codes 4xx ou 5xx
            
            run_data = response.json()
            run_id = run_data.get('data', {}).get('id')
            
            if not run_id:
                raise TikTokScraperError("Impossible de récupérer l'ID du run dans la réponse de l'API.")
                
            print(f"✅ Scraper lancé avec l'ID: {run_id}")
            return run_id
            
        except requests.exceptions.RequestException as e:
            # Catch network errors, connection issues, HTTP errors
            error_msg = f"Erreur réseau ou API lors du lancement du scraper: {e}"
            if response is not None:
                error_msg += f" (Status Code: {response.status_code}, Response: {response.text[:200]}...)"
            raise TikTokScraperError(error_msg) from e
        except json.JSONDecodeError as e:
             raise TikTokScraperError(f"Erreur lors du décodage JSON de la réponse de lancement: {e}") from e
        except Exception as e:
            # Catch any other unexpected errors
            raise TikTokScraperError(f"Une erreur inattendue s'est produite lors du lancement: {e}") from e
            
    def _wait_for_completion(self, run_id: str, timeout: int = 300, check_interval: int = 5) -> Dict:
        """
        Attend la fin de l'exécution du scraper
        
        Args:
            run_id (str): ID de l'exécution
            timeout (int): Timeout en secondes (défaut: 300s)
            check_interval (int): Intervalle de vérification en secondes (défaut: 5s)
            
        Returns:
            Dict: Données du statut final de l'exécution
            
        Raises:
            TikTokScraperError: Si l'exécution échoue ou dépasse le timeout
        """
        start_time = time.time()
        url = f'{self.base_url}/actor-runs/{run_id}?token={self.apify_token}'
        
        print("⏳ Attente de la fin de l'exécution...")

        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                raise TikTokScraperError(f"Timeout dépassé ({timeout}s) en attendant la fin de l'exécution {run_id}")
                
            try:
                response = requests.get(url)
                response.raise_for_status() # Lève une exception pour les codes 4xx ou 5xx
                
                run_status_data = response.json().get('data', {})
                status = run_status_data.get('status')
                
                if not status:
                     print(f"⚠️ Warning: 'status' field missing in run status response for {run_id}.")
                     # Continue waiting, or potentially raise error after a few attempts

                print(f"🔄 Statut actuel ({int(elapsed_time)}s): {status}")
                
                if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                    break
                    
            except requests.exceptions.RequestException as e:
                 print(f"⚠️ Warning: Erreur réseau ou API lors de la vérification du statut de {run_id}: {e}. Réessai...")
                 # Don't re-raise immediately, maybe it's transient. Sleep and retry.
            except json.JSONDecodeError as e:
                 print(f"⚠️ Warning: Erreur JSON lors de la vérification du statut de {run_id}: {e}. Réessai...")
                 # Sleep and retry
            except Exception as e:
                 # Catch other unexpected errors during status check
                 print(f"⚠️ Warning: Une erreur inattendue s'est produite lors de la vérification du statut de {run_id}: {e}. Réessai...")
                 # Sleep and retry

            time.sleep(check_interval)
        
        if status != 'SUCCEEDED':
            raise TikTokScraperError(f"Le scraper a échoué ou n'a pas réussi. Statut final pour {run_id}: {status}")
            
        print(f"✅ Exécution {run_id} terminée avec succès. Statut: {status}")
        return run_status_data
    
    def _fetch_dataset(self, dataset_id: str) -> List[Dict]:
        """
        Récupère les données du dataset
        
        Args:
            dataset_id (str): ID du dataset
            
        Returns:
            List[Dict]: Données brutes du dataset
            
        Raises:
            TikTokScraperError: Si la récupération échoue
        """
        print(f"📥 Récupération des données du dataset {dataset_id}...")
        url = f'{self.base_url}/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
        try:
            response = requests.get(url)
            response.raise_for_status() # Lève une exception pour les codes 4xx ou 5xx
            
            data = response.json()
            print(f"✅ Données récupérées avec succès. Nombre d'éléments: {len(data)}")
            return data
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Erreur réseau ou API lors de la récupération des données du dataset {dataset_id}: {e}"
            if response is not None:
                error_msg += f" (Status Code: {response.status_code}, Response: {response.text[:200]}...)"
            raise TikTokScraperError(error_msg) from e
        except json.JSONDecodeError as e:
            raise TikTokScraperError(f"Erreur lors du décodage JSON des données du dataset {dataset_id}: {e}") from e
        except Exception as e:
             raise TikTokScraperError(f"Une erreur inattendue s'est produite lors de la récupération des données du dataset {dataset_id}: {e}") from e

    
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
        if not isinstance(data, dict):
            return default # Handle cases where data is not even a dictionary
        
        try:
            value = data
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key, default)
                elif isinstance(value, list) and isinstance(key, int) and len(value) > key:
                    value = value[key] # Basic list index support if needed, though not used here
                else:
                    return default # Key not found or data is not a dict/list at this level
                
                if value is default and key != keys[-1]:
                     # If we got the default value *before* the last key, the path is broken
                     return default
                     
            return value
        except (TypeError, IndexError, AttributeError):
            # Catch potential errors if an intermediate step isn't a dict/list as expected
            return default

    def _transform_profile_data(self, raw_profile: Dict) -> Optional[Dict]:
        """
        Transforme les données brutes du profil en format standardisé
        
        Args:
            raw_profile (Dict): Données brutes du profil
            
        Returns:
            Optional[Dict]: Profil formaté ou None si les données brutes sont insuffisantes/invalides
        """
        if not isinstance(raw_profile, dict) or not raw_profile:
             print(f"⚠️ Skipping invalid raw profile data: {raw_profile}")
             return None
             
        author_meta = raw_profile.get("authorMeta", {})
        if not author_meta:
             print(f"⚠️ Skipping profile data with missing 'authorMeta': {raw_profile}")
             return None

        # Check for essential fields
        profile_id = self._safe_get_nested(author_meta, "id")
        profile_name = self._safe_get_nested(author_meta, "nickName")
        if not profile_id or not profile_name:
             print(f"⚠️ Skipping profile with missing ID or Nickname: {raw_profile}")
             return None

        transformed_data = {
            "platform": "tiktok",
            "profile_id": profile_id,
            "profile_name": profile_name,
            "page_name": self._safe_get_nested(author_meta, "name", ""), # Ensure default is same type
            "url": self._safe_get_nested(author_meta, "profileUrl", ""),
            "biography": self._safe_get_nested(author_meta, "signature", ""),
            "metrics": {
                "likes": self._safe_get_nested(author_meta, "heart", 0),
                "followers": self._safe_get_nested(author_meta, "fans", 0),
                # Add other metrics if available and needed, e.g., videoCount
                "video_count": self._safe_get_nested(author_meta, "video", 0),
            },
            "is_verified": self._safe_get_nested(author_meta, "verified", False),
            "scraped_at": datetime.now().isoformat()
        }
        
        return transformed_data
    
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
            TikTokScraperError: Si le processus de scraping échoue à une étape critique.
        """
        # Convertir en liste si c'est une chaîne
        if isinstance(profiles, str):
            profiles = [profiles]
            
        # Input validation handled in _prepare_actor_input
        
        tiktok_profiles = []
        run_id = None # Initialize run_id outside try block
        
        try:
            # 1. Préparer les paramètres (validation incluse)
            actor_input = self._prepare_actor_input(
                profiles=profiles,
                results_per_page=results_per_page
            )
            
            # 2. Lancer le scraper
            run_id = self._start_scraping_run(actor_input)
            
            # 3. Attendre la fin
            run_status_data = self._wait_for_completion(run_id, timeout)
            
            # 4. Récupérer les données
            dataset_id = run_status_data.get('defaultDatasetId')
            if not dataset_id:
                raise TikTokScraperError(f"Impossible de récupérer l'ID du dataset pour l'exécution {run_id}. Aucune donnée disponible.")
                
            raw_data = self._fetch_dataset(dataset_id)
            
            # 5. Transformer les données
            print("✨ Transformation des données des profils...")
            for i, raw_profile in enumerate(raw_data):
                try:
                    transformed_profile = self._transform_profile_data(raw_profile)
                    if transformed_profile: # Only add if transformation was successful
                        tiktok_profiles.append(transformed_profile)
                except Exception as e:
                    # Catch unexpected errors during transformation of a single profile
                    print(f"⚠️ Erreur lors de la transformation du profil {i+1}: {e}. Skipping this profile.")
                    # Continue with the next profile
                    
            print(f"✅ {len(tiktok_profiles)} profil(s) TikTok traité(s) avec succès sur {len(raw_data)} éléments bruts.")
            
            # 6. Sauvegarder si demandé
            if save_to_file:
                self.save_profiles(tiktok_profiles, save_to_file)
                
        except TikTokScraperError as e:
            # Catch exceptions raised by helper methods
            print(f"❌ Une erreur est survenue pendant le processus de scraping: {e}")
            # Depending on requirement, you might want to return partial results or an empty list
            # For now, we re-raise or return empty based on severity. Let's return empty.
            return [] # Indicate failure by returning an empty list

        except Exception as e:
            # Catch any other unexpected exceptions during the orchestration
            print(f"❌ Une erreur inattendue est survenue dans scrape_profiles: {e}")
            # You might want to inspect the run on Apify if run_id is available
            if run_id:
                print(f"Veuillez vérifier l'état de l'exécution Apify {run_id} pour plus de détails.")
            return [] # Indicate failure

        return tiktok_profiles
    
    def save_profiles(self, profiles: List[Dict], filename: str) -> None:
        """
        Sauvegarde les profils dans un fichier JSON
        
        Args:
            profiles (List[Dict]): Liste des profils à sauvegarder
            filename (str): Nom du fichier de sauvegarde
            
        Raises:
            TikTokScraperError: Si la sauvegarde échoue.
        """
        if not filename:
             raise TikTokScraperError("Filename for saving cannot be empty.")
             
        try:
            # Ensure directory exists if filename contains a path
            dirname = os.path.dirname(filename)
            if dirname and not os.path.exists(dirname):
                 os.makedirs(dirname)
                 print(f"📂 Création du répertoire: {dirname}")
                 
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(profiles, f, ensure_ascii=False, indent=2)
            print(f"💾 Profils TikTok sauvegardés dans '{filename}'")
            
        except (IOError, OSError) as e:
            raise TikTokScraperError(f"Erreur lors de la sauvegarde du fichier '{filename}': {e}") from e
        except TypeError as e:
             raise TikTokScraperError(f"Erreur de type lors de la sérialisation JSON du fichier '{filename}': {e}. Les données ne sont peut-être pas JSON sérialisables.") from e
        except Exception as e:
             raise TikTokScraperError(f"Une erreur inattendue s'est produite lors de la sauvegarde du fichier '{filename}': {e}") from e


    def get_profiles_summary(self, profiles: List[Dict]) -> None:
        """
        Affiche un résumé des profils TikTok scrapés
        
        Args:
            profiles (List[Dict]): Liste des profils
        """
        if not profiles:
            print("\nPas de profils à afficher.")
            return

        print("\n📊 RÉSUMÉ DES PROFILS TIKTOK:")
        print("=" * 60)
        
        for profile in profiles:
            # Use _safe_get_nested or .get() for robustness, though data should be clean here
            profile_name = self._safe_get_nested(profile, 'profile_name', 'N/A')
            profile_id = self._safe_get_nested(profile, 'profile_id', 'N/A')
            url = self._safe_get_nested(profile, 'url', 'N/A')
            followers = self._safe_get_nested(profile, 'metrics', 'followers', 0)
            likes = self._safe_get_nested(profile, 'metrics', 'likes', 0)
            is_verified = self._safe_get_nested(profile, 'is_verified', False)
            biography = self._safe_get_nested(profile, 'biography', '')

            if profile_name != 'N/A':
                print(f"   📝 Nom d'affichage: {profile_name}")
            if profile_id != 'N/A':
                 print(f"   🆔 ID: {profile_id}")
            if url != 'N/A':
                print(f"   🔗 URL: {url}")
            print(f"   👥 Followers: {followers:,}") # Use :, for comma formatting
            print(f"   💖 Likes totaux: {likes:,}")
            print(f"   ✅ Vérifié: {'Oui' if is_verified else 'Non'}")
            if biography:
                bio_preview = biography[:80] + "..." if len(biography) > 80 else biography
                print(f"   📄 Bio: {bio_preview}")
            print("-" * 60)
    
    def scrape_telecom_profiles(self, 
                              # oldest_post_date is not supported by this Apify actor input,
                              # so let's remove it or note it if it were relevant for other actors
                              # oldest_post_date: Optional[str] = None, 
                              save_to_file: Optional[str] = None,
                              timeout: int = 300) -> List[Dict]:
        """
        Méthode de convenance pour scraper les profils TikTok des opérateurs télécoms marocains
        
        Args:
            # oldest_post_date (Optional[str]): Date la plus ancienne (YYYY-MM-DD) - Not applicable for this actor
            save_to_file (Optional[str]): Fichier de sauvegarde
            timeout (int): Timeout en secondes pour l'exécution
            
        Returns:
            List[Dict]: Profils des opérateurs télécoms (peut être vide en cas d'erreur)
        """
        telecom_profiles = [
            "orangemaroc",
            "inwi.maroc", 
            "maroctelecom"
        ]
        
        print("\n🤖 Lancement du scraping pour les profils télécoms marocains...")

        # Note: results_per_page=1 is sufficient for just getting profile info,
        # as the actor returns only one item per profile requested in the 'profiles' list.
        return self.scrape_profiles(
            profiles=telecom_profiles,
            results_per_page=1, # This actor input seems to control video results, not profile count per se
            timeout=timeout,
            save_to_file=save_to_file
            # oldest_post_date=oldest_post_date # Remove this as it's not a valid input for this actor
        )
