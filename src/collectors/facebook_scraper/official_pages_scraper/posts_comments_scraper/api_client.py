import requests
import time
import json
import requests.exceptions
from typing import Optional, Dict, List, Any


class ApifyAPIClient:
    """
    Client pour interagir avec l'API Apify.
    Gère les appels API, l'exécution des acteurs et la récupération des datasets.
    """

    def __init__(self, apify_token: str):
        """
        Initialise le client API Apify.

        Args:
            apify_token (str): Token d'authentification Apify

        Raises:
            ValueError: Si le token est invalide
        """
        if not apify_token or not isinstance(apify_token, str):
            raise ValueError("apify_token must be a non-empty string.")
        
        self.apify_token = apify_token
        self.base_url = "https://api.apify.com/v2"

    def run_actor(self, actor_id: str, actor_input: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Lance un acteur Apify et attend sa completion.

        Args:
            actor_id (str): ID de l'acteur Apify
            actor_input (dict): Paramètres d'entrée pour l'acteur

        Returns:
            dict: Données du run ou None si échec
        """
        print(f"🚀 Lancement de l'acteur {actor_id}...")

        try:
            start_response = requests.post(
                f'{self.base_url}/acts/{actor_id}/runs?token={self.apify_token}',
                json=actor_input
            )
            start_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur réseau ou HTTP lors du lancement de l'acteur {actor_id}: {e}")
            return None

        run_data = start_response.json()
        run_id = run_data.get('data', {}).get('id')

        if not run_id:
            print(f"❌ Impossible de récupérer l'ID du run pour l'acteur {actor_id}.")
            return None

        return self._wait_for_completion(run_id, actor_id)

    def _wait_for_completion(self, run_id: str, actor_id: str) -> Optional[Dict[str, Any]]:
        """
        Attend la completion d'un run d'acteur.

        Args:
            run_id (str): ID du run
            actor_id (str): ID de l'acteur (pour les logs)

        Returns:
            dict: Données du run ou None si échec
        """
        print(f"⏳ Exécution en cours (Run ID: {run_id})...")
        
        while True:
            try:
                run_status_response = requests.get(
                    f'{self.base_url}/actor-runs/{run_id}?token={self.apify_token}'
                )
                run_status_response.raise_for_status()
                run_status_data = run_status_response.json().get('data', {})
                status = run_status_data.get('status')

                print(f"🔄 Statut : {status}")
                if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                    break

            except requests.exceptions.RequestException as e:
                print(f"⚠️ Erreur réseau lors de la vérification du statut du run {run_id}: {e}. Réessai...")
                status = 'RETRYING'

            if status == 'RETRYING':
                time.sleep(10)
            elif status not in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                time.sleep(5)

        if status != 'SUCCEEDED':
            print(f"❌ Échec de l'exécution de l'acteur {actor_id}. Statut final : {status}")
            return None

        print(f"✅ Acteur {actor_id} exécuté avec succès.")
        return run_status_data

    def get_dataset_items(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Récupère les éléments d'un dataset Apify.

        Args:
            dataset_id (str): ID du dataset

        Returns:
            list: Liste des éléments du dataset ou liste vide si échec
        """
        print(f"📦 Récupération des données du dataset {dataset_id}...")
        
        try:
            dataset_response = requests.get(
                f'{self.base_url}/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
            )
            dataset_response.raise_for_status()
            return dataset_response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur réseau lors de la récupération du dataset {dataset_id}: {e}")
            return []
        except json.JSONDecodeError:
            print(f"❌ Erreur de décodage JSON pour le dataset {dataset_id}.")
            return []

    def collect_posts_data(self, facebook_url: str, max_posts: int, from_date: str) -> List[Dict[str, Any]]:
        """
        Collecte les données de posts Facebook via l'acteur Apify.

        Args:
            facebook_url (str): URL de la page Facebook
            max_posts (int): Nombre maximum de posts
            from_date (str): Date de début au format ISO

        Returns:
            list: Données brutes des posts
        """
        posts_actor_id = 'apify~facebook-posts-scraper'
        
        actor_input = {
            "startUrls": [{"url": facebook_url}],
            "resultsLimit": max_posts,
            "mode": "page",
            "commentsMode": "none",
            "reactions": True,
            "proxyConfig": {"useApifyProxy": True},
            "captionText": False
        }

        run_data = self.run_actor(posts_actor_id, actor_input)
        if not run_data:
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des posts.")
            return []

        return self.get_dataset_items(dataset_id)

    def collect_comments_data(self, post_url: str, max_comments: int) -> List[Dict[str, Any]]:
        """
        Collecte les données de commentaires pour un post via l'acteur Apify.

        Args:
            post_url (str): URL du post Facebook
            max_comments (int): Nombre maximum de commentaires

        Returns:
            list: Données brutes des commentaires
        """
        if not post_url:
            return []

        comments_actor_id = 'apify~facebook-comments-scraper'
        
        actor_input = {
            "startUrls": [{"url": post_url}],
            "resultsLimit": max_comments,
            "reactions": True,
            "proxyConfig": {"useApifyProxy": True}
        }

        run_data = self.run_actor(comments_actor_id, actor_input)
        if not run_data:
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print(f"❌ Impossible de récupérer l'ID du dataset des commentaires pour {post_url}.")
            return []

        return self.get_dataset_items(dataset_id)