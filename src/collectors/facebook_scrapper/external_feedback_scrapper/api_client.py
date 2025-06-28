import requests
import time
from requests.exceptions import RequestException
from json.decoder import JSONDecodeError


class ApifyAPIClient:
    """
    Client pour interagir avec l'API Apify.
    Gère les appels aux acteurs et la récupération des datasets.
    """
    
    def __init__(self, apify_token):
        """
        Initialize the Apify API client.
        
        Args:
            apify_token (str): Apify API token
        """
        if not apify_token:
            raise ValueError("Apify token cannot be None or empty.")
        
        self.apify_token = apify_token
        self.base_url = "https://api.apify.com/v2"
    
    def run_actor(self, actor_id, actor_input):
        """
        Run an Apify actor and wait for completion.
        
        Args:
            actor_id (str): Apify actor ID
            actor_input (dict): Input parameters for the actor
            
        Returns:
            dict: Actor run data (status, datasetId, etc.) or None if failed
        """
        print(f"🚀 Lancement de l'acteur {actor_id}...")
        
        try:
            start_response = requests.post(
                f'{self.base_url}/acts/{actor_id}/runs?token={self.apify_token}',
                json=actor_input
            )
            start_response.raise_for_status()
            
        except RequestException as e:
            print(f"❌ Erreur réseau ou HTTP lors du lancement de l'acteur {actor_id}: {e}")
            return None
        except JSONDecodeError as e:
            print(f"❌ Erreur de décodage JSON lors du lancement de l'acteur {actor_id}: {e}")
            return None
        
        try:
            run_data = start_response.json()
            run_id = run_data.get('data', {}).get('id')
        except JSONDecodeError as e:
            print(f"❌ Erreur de décodage JSON dans la réponse de lancement pour l'acteur {actor_id}: {e}")
            return None
        
        if not run_id:
            print(f"❌ Impossible de récupérer l'ID du run pour l'acteur {actor_id}.")
            return None
        
        return self._wait_for_completion(run_id)
    
    def _wait_for_completion(self, run_id):
        """
        Wait for an actor run to complete.
        
        Args:
            run_id (str): Run ID to monitor
            
        Returns:
            dict: Final run status data or None if failed
        """
        print(f"⏳ Exécution du run {run_id} en cours...")
        
        while True:
            try:
                run_status_response = requests.get(
                    f'{self.base_url}/actor-runs/{run_id}?token={self.apify_token}'
                )
                run_status_response.raise_for_status()
                
                run_status_data = run_status_response.json().get('data', {})
                status = run_status_data.get('status')
                
            except RequestException as e:
                print(f"❌ Erreur réseau ou HTTP lors de la vérification du statut du run {run_id}: {e}")
                return None
            except JSONDecodeError as e:
                print(f"❌ Erreur de décodage JSON dans la réponse de statut pour le run {run_id}: {e}")
                return None
            except Exception as e:
                print(f"❌ Erreur inattendue lors de la vérification du statut du run {run_id}: {e}")
                return None
            
            print(f"🔄 Statut du run {run_id}: {status}")
            
            # Break the loop if the run reaches a final state
            if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                break
            elif status in ['RUNNING', 'READY']:
                # Still running or ready to run, wait
                pass
            else:
                # Handle unexpected status values
                print(f"⚠️ Statut du run {run_id} inattendu: {status}. Réessai dans 5s.")
            
            time.sleep(5)
        
        if status != 'SUCCEEDED':
            print(f"❌ Échec de l'exécution du run {run_id}. Statut final : {status}")
            return run_status_data
        
        print(f"✅ Run {run_id} terminé avec succès.")
        return run_status_data
    
    def get_dataset_items(self, dataset_id):
        """
        Retrieve items from an Apify dataset.
        
        Args:
            dataset_id (str): Dataset ID
            
        Returns:
            list: Dataset items or empty list if failed
        """
        if not dataset_id:
            print("⚠️ L'ID du dataset est manquant. Impossible de récupérer les items.")
            return []
        
        print(f"⬇️ Récupération des items du dataset {dataset_id}...")
        
        try:
            dataset_response = requests.get(
                f'{self.base_url}/datasets/{dataset_id}/items?token={self.apify_token}&clean=true&format=json'
            )
            dataset_response.raise_for_status()
            
        except RequestException as e:
            print(f"❌ Erreur réseau ou HTTP lors de la récupération des données du dataset {dataset_id}: {e}")
            return []
        
        try:
            return dataset_response.json()
        except JSONDecodeError as e:
            print(f"❌ Erreur de décodage JSON lors de la lecture des items du dataset {dataset_id}: {e}")
            return []
        except Exception as e:
            print(f"❌ Erreur inattendue lors de la récupération des items du dataset {dataset_id}: {e}")
            return []


class FacebookDataAPI:
    """
    Wrapper spécialisé pour les données Facebook via les acteurs Apify.
    """
    
    def __init__(self, apify_client):
        """
        Initialize Facebook data API wrapper.
        
        Args:
            apify_client (ApifyAPIClient): Instance of Apify API client
        """
        self.client = apify_client
        self.posts_actor_id = 'easyapi~facebook-posts-search-scraper'
        self.comments_actor_id = 'apify~facebook-comments-scraper'
    
    def get_posts_data(self, search_query, max_posts=2):
        """
        Retrieve Facebook posts data.
        
        Args:
            search_query (str): Search query for posts
            max_posts (int): Maximum number of posts to collect
            
        Returns:
            list: Raw posts data from API or empty list if failed
        """
        print(f"\n--- Récupération des données de posts ---")
        print(f"📱 Requête: '{search_query}', Max posts: {max_posts}")
        
        actor_input = {
            "searchQuery": search_query,
            "maxPosts": max_posts,
            "proxyConfig": {"useApifyProxy": True},
        }
        
        run_data = self.client.run_actor(self.posts_actor_id, actor_input)
        if not run_data or run_data.get('status') != 'SUCCEEDED':
            print(f"❌ L'acteur de posts a échoué.")
            return []
        
        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des posts.")
            return []
        
        posts_data = self.client.get_dataset_items(dataset_id)
        print(f"✅ {len(posts_data)} posts récupérés depuis l'API.")
        
        return posts_data
    
    def get_comments_data(self, post_url, max_comments=3):
        """
        Retrieve Facebook comments data for a specific post.
        
        Args:
            post_url (str): URL of the Facebook post
            max_comments (int): Maximum number of comments to collect
            
        Returns:
            list: Raw comments data from API or empty list if failed
        """
        if not post_url or not isinstance(post_url, str):
            print("⚠️ URL du post invalide pour la collecte des commentaires.")
            return []
        
        print(f"💬 Récupération des commentaires pour : {post_url}")
        
        actor_input = {
            "startUrls": [{"url": post_url}],
            "resultsLimit": max_comments,
            "reactions": True,
            "proxyConfig": {"useApifyProxy": True},
        }
        
        run_data = self.client.run_actor(self.comments_actor_id, actor_input)
        if not run_data or run_data.get('status') != 'SUCCEEDED':
            print(f"❌ L'acteur de commentaires a échoué pour l'URL {post_url}.")
            return []
        
        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print(f"❌ Impossible de récupérer l'ID du dataset des commentaires.")
            return []
        
        comments_data = self.client.get_dataset_items(dataset_id)
        print(f"✅ {len(comments_data)} commentaires récupérés depuis l'API.")
        
        return comments_data