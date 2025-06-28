import unittest
from unittest.mock import patch, Mock, MagicMock
import requests
import time
import json
from urllib.parse import urlparse # Nécessaire pour le code original
from typing import List, Dict
from src.collectors.facebook_scrapper.official_pages_scraper.page_scraper.api_client import *

class TestApifyAPIClient(unittest.TestCase):

    def setUp(self):
        """Configuration avant chaque test"""
        self.apify_token = "test_token_123"
        self.client = ApifyAPIClient(self.apify_token)
        self.base_url = self.client.base_url
        self.actor_id = self.client.actor_id

    def test_init_success(self):
        """Test l'initialisation réussie du client"""
        client = ApifyAPIClient("some_token")
        self.assertEqual(client.apify_token, "some_token")
        self.assertEqual(client.actor_id, 'apify~facebook-pages-scraper')
        self.assertEqual(client.base_url, 'https://api.apify.com/v2')

    def test_init_no_token(self):
        """Test l'initialisation sans token (doit lever une erreur)"""
        with self.assertRaisesRegex(ValueError, "Apify token cannot be empty."):
            ApifyAPIClient("")

    def test_normalize_facebook_url_http(self):
        """Test la normalisation d'une URL HTTP Facebook"""
        url = "http://facebook.com/apifymain"
        expected = "https://web.facebook.com/apifymain"
        self.assertEqual(self.client._normalize_facebook_url(url), expected)

    def test_normalize_facebook_url_https(self):
        """Test la normalisation d'une URL HTTPS Facebook"""
        url = "https://www.facebook.com/apifymain"
        expected = "https://web.facebook.com/apifymain"
        self.assertEqual(self.client._normalize_facebook_url(url), expected)

    def test_normalize_facebook_url_web_already(self):
        """Test la normalisation d'une URL web.facebook.com (ne doit pas changer)"""
        url = "https://web.facebook.com/apifymain"
        expected = "https://web.facebook.com/apifymain"
        self.assertEqual(self.client._normalize_facebook_url(url), expected)

    def test_normalize_facebook_url_no_protocol(self):
        """Test la normalisation d'une URL Facebook sans protocole"""
        url = "facebook.com/apifymain"
        expected = "https://web.facebook.com/apifymain"
        self.assertEqual(self.client._normalize_facebook_url(url), expected)

    def test_normalize_facebook_url_other_site(self):
        """Test la normalisation d'une URL non Facebook (doit juste ajouter HTTPS)"""
        url = "google.com"
        expected = "https://google.com"
        self.assertEqual(self.client._normalize_facebook_url(url), expected)

    def test_normalize_facebook_url_with_query_fragment(self):
        """Test normalisation avec query params et fragment"""
        url = "http://m.facebook.com/page?id=123¶m=value#section"
        expected = "https://web.facebook.com/page?id=123¶m=value#section"
        self.assertEqual(self.client._normalize_facebook_url(url), expected)

    @patch.object(ApifyAPIClient, '_normalize_facebook_url', side_effect=lambda x: f"normalized_{x}")
    def test_prepare_start_urls(self, mock_normalize):
        """Test la préparation des URLs de démarrage"""
        urls = ["url1", "url2"]
        expected = [{"url": "normalized_url1", "method": "GET"}, {"url": "normalized_url2", "method": "GET"}]
        result = self.client._prepare_start_urls(urls)

        self.assertEqual(result, expected)
        mock_normalize.assert_any_call("url1")
        mock_normalize.assert_any_call("url2")
        self.assertEqual(mock_normalize.call_count, 2)

    def test_prepare_start_urls_empty(self):
        """Test la préparation avec une liste d'URLs vide"""
        urls = []
        expected = []
        result = self.client._prepare_start_urls(urls)
        self.assertEqual(result, expected)

    @patch('requests.post')
    @patch.object(ApifyAPIClient, '_prepare_start_urls')
    def test_start_scraping_run_success(self, mock_prepare_urls, mock_requests_post):
        """Test le lancement réussi d'une exécution"""
        mock_prepare_urls.return_value = [{"url": "normalized_url", "method": "GET"}]
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'id': 'test_run_123'}}
        mock_response.raise_for_status.return_value = None # No exception on success
        mock_requests_post.return_value = mock_response

        urls_to_scrape = ["http://facebook.com/page"]
        run_id = self.client.start_scraping_run(urls_to_scrape)

        self.assertEqual(run_id, 'test_run_123')
        mock_prepare_urls.assert_called_once_with(urls_to_scrape)
        mock_requests_post.assert_called_once_with(
            f'{self.base_url}/acts/{self.actor_id}/runs?token={self.apify_token}',
            json={"startUrls": [{"url": "normalized_url", "method": "GET"}]}
        )
        mock_response.raise_for_status.assert_called_once()

    def test_start_scraping_run_no_urls(self):
        """Test le lancement sans URL (doit lever une erreur)"""
        with self.assertRaisesRegex(ValueError, "Aucune URL fournie pour le scraping."):
            self.client.start_scraping_run([])

    @patch('requests.post')
    @patch.object(ApifyAPIClient, '_prepare_start_urls', return_value=[])
    def test_start_scraping_run_no_valid_urls_after_prep(self, mock_prepare_urls, mock_requests_post):
        """Test le lancement quand _prepare_start_urls renvoie une liste vide"""
        with self.assertRaisesRegex(ValueError, "Aucune URL valide n'a été préparée pour le scraping."):
             self.client.start_scraping_run(["invalid_url", "another_one"])

        mock_prepare_urls.assert_called_once() # Should be called
        mock_requests_post.assert_not_called() # Should not proceed to request

    @patch('requests.post')
    @patch.object(ApifyAPIClient, '_prepare_start_urls', return_value=[{"url": "url", "method": "GET"}])
    def test_start_scraping_run_api_error(self, mock_prepare_urls, mock_requests_post):
        """Test l'erreur API lors du lancement"""
        mock_requests_post.side_effect = requests.exceptions.RequestException("Connection error")

        with self.assertRaisesRegex(Exception, "Erreur de requête API lors du lancement du scraper: Connection error"):
            self.client.start_scraping_run(["url"])

        mock_prepare_urls.assert_called_once()
        mock_requests_post.assert_called_once() # Check it was called before the exception

    @patch('requests.post')
    @patch.object(ApifyAPIClient, '_prepare_start_urls', return_value=[{"url": "url", "method": "GET"}])
    def test_start_scraping_run_api_non_200(self, mock_prepare_urls, mock_requests_post):
        """Test la réponse API non 2xx lors du lancement"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Bad Request")
        mock_requests_post.return_value = mock_response

        with self.assertRaisesRegex(Exception, "Erreur de requête API lors du lancement du scraper: Bad Request"):
            self.client.start_scraping_run(["url"])

        mock_prepare_urls.assert_called_once()
        mock_requests_post.assert_called_once()
        mock_response.raise_for_status.assert_called_once()

    @patch('requests.post')
    @patch.object(ApifyAPIClient, '_prepare_start_urls', return_value=[{"url": "url", "method": "GET"}])
    def test_start_scraping_run_missing_run_id(self, mock_prepare_urls, mock_requests_post):
        """Test la réponse API sans ID de run"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {}} # Missing 'id'
        mock_response.text = "Response without ID" # For printing in the error case
        mock_response.raise_for_status.return_value = None
        mock_requests_post.return_value = mock_response

        with self.assertRaisesRegex(Exception, "Impossible de récupérer l'ID du run depuis la réponse API."):
            # Also capture stdout to verify the print statement if needed, but regex is sufficient
            self.client.start_scraping_run(["url"])

        mock_prepare_urls.assert_called_once()
        mock_requests_post.assert_called_once()

    @patch('requests.get')
    def test_get_run_status_success(self, mock_requests_get):
        """Test la récupération réussie du statut"""
        run_id = "test_run_123"
        status_data = {'id': run_id, 'status': 'RUNNING'}
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': status_data}
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        result = self.client.get_run_status(run_id)

        self.assertEqual(result, status_data)
        mock_requests_get.assert_called_once_with(
            f'{self.base_url}/actor-runs/{run_id}?token={self.apify_token}'
        )
        mock_response.raise_for_status.assert_called_once()

    @patch('requests.get')
    def test_get_run_status_api_error(self, mock_requests_get):
        """Test l'erreur API lors de la récupération du statut"""
        run_id = "test_run_123"
        mock_requests_get.side_effect = requests.exceptions.RequestException("Connection error")

        with self.assertRaisesRegex(Exception, f"Erreur lors de la vérification du statut de l'exécution {run_id}: Connection error"):
            self.client.get_run_status(run_id)

        mock_requests_get.assert_called_once()

    @patch('requests.get')
    def test_get_run_status_api_non_200(self, mock_requests_get):
        """Test la réponse API non 2xx lors de la récupération du statut"""
        run_id = "test_run_123"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
        mock_requests_get.return_value = mock_response

        with self.assertRaisesRegex(Exception, f"Erreur lors de la vérification du statut de l'exécution {run_id}: Not Found"):
            self.client.get_run_status(run_id)

        mock_requests_get.assert_called_once()
        mock_response.raise_for_status.assert_called_once()


    @patch.object(ApifyAPIClient, 'get_run_status')
    @patch('time.sleep')
    @patch('time.time', side_effect=[0, 5, 10, 15]) # Simulate time passing
    def test_wait_for_completion_success(self, mock_time, mock_sleep, mock_get_status):
        """Test l'attente jusqu'à la complétion réussie"""
        run_id = "test_run_123"
        # Simulate status changes: RUNNING, RUNNING, SUCCEEDED
        mock_get_status.side_effect = [
            {'id': run_id, 'status': 'RUNNING'},
            {'id': run_id, 'status': 'RUNNING'},
            {'id': run_id, 'status': 'SUCCEEDED', 'datasetId': 'test_dataset_456'} # Final successful status
        ]
        check_interval = 5
        timeout = 30 # Well within timeout

        result = self.client.wait_for_completion(run_id, timeout=timeout, check_interval=check_interval)

        self.assertEqual(result, {'id': run_id, 'status': 'SUCCEEDED', 'datasetId': 'test_dataset_456'})
        self.assertEqual(mock_get_status.call_count, 3) # Called until SUCCEEDED
        mock_get_status.assert_any_call(run_id)
        # Sleep should be called twice after the first two RUNNING status checks
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(check_interval)
        # time.time called at start and before each status check + before timeout check inside loop
        # It's called 3 times inside loop (before get_run_status) + 1 at start = 4 times
        # The side_effect list provides values, so it gets called as many times as needed
        self.assertGreaterEqual(mock_time.call_count, 4)


    @patch.object(ApifyAPIClient, 'get_run_status')
    @patch('time.sleep')
    @patch('time.time', side_effect=[0, 5, 10, 15, 20, 25, 31]) # Simulate time exceeding timeout
    def test_wait_for_completion_timeout(self, mock_time, mock_sleep, mock_get_status):
        """Test l'attente jusqu'au timeout"""
        run_id = "test_run_123"
        # Always return RUNNING
        mock_get_status.return_value = {'id': run_id, 'status': 'RUNNING'}
        check_interval = 5
        timeout = 30

        with self.assertRaisesRegex(TimeoutError, f"Timeout dépassé \\({timeout}s\\) en attente de l'exécution {run_id}."):
            self.client.wait_for_completion(run_id, timeout=timeout, check_interval=check_interval)

        # Should try get_run_status until timeout is exceeded
        # So get_run_status is called 6 times. sleep is called 5 times.
        self.assertEqual(mock_get_status.call_count, 5)
        mock_get_status.assert_any_call(run_id)
        self.assertEqual(mock_sleep.call_count, 5) # 5 calls to sleep
        mock_sleep.assert_any_call(check_interval)
        self.assertGreaterEqual(mock_time.call_count, 7) # Start time + checks inside loop

    @patch.object(ApifyAPIClient, 'get_run_status')
    @patch('time.sleep')
    @patch('time.time', side_effect=[0, 5, 10]) # Simulate time passing
    def test_wait_for_completion_failed(self, mock_time, mock_sleep, mock_get_status):
        """Test l'attente jusqu'à un statut d'échec"""
        run_id = "test_run_123"
        # Simulate status changes: RUNNING, FAILED
        mock_get_status.side_effect = [
            {'id': run_id, 'status': 'RUNNING'},
            {'id': run_id, 'status': 'FAILED'}
        ]
        check_interval = 5
        timeout = 30

        with self.assertRaisesRegex(RuntimeError, "Le scraper a échoué ou a été interrompu. Statut final: FAILED"):
            self.client.wait_for_completion(run_id, timeout=timeout, check_interval=check_interval)

        self.assertEqual(mock_get_status.call_count, 2) # Called until FAILED
        mock_get_status.assert_any_call(run_id)
        self.assertEqual(mock_sleep.call_count, 1) # Sleep called once after the first RUNNING check
        mock_sleep.assert_any_call(check_interval)
        self.assertGreaterEqual(mock_time.call_count, 3)

    @patch.object(ApifyAPIClient, 'get_run_status')
    @patch('time.sleep')
    @patch('time.time', side_effect=[0, 5, 10, 15])
    @patch('builtins.print') # Mock print to avoid output during test
    def test_wait_for_completion_error_then_success(self, mock_print, mock_time, mock_sleep, mock_get_status):
        """Test l'attente gérant une erreur API temporaire puis succès"""
        run_id = "test_run_123"
        # Simulate status changes: RUNNING, API Error, SUCCEEDED
        mock_get_status.side_effect = [
            {'id': run_id, 'status': 'RUNNING'},
            Exception("Simulated temporary API error"), # get_run_status raises exception
            {'id': run_id, 'status': 'SUCCEEDED', 'datasetId': 'test_dataset_456'} # Recovery and success
        ]
        check_interval = 5
        timeout = 30

        result = self.client.wait_for_completion(run_id, timeout=timeout, check_interval=check_interval)

        self.assertEqual(result, {'id': run_id, 'status': 'SUCCEEDED', 'datasetId': 'test_dataset_456'})
        self.assertEqual(mock_get_status.call_count, 3) # Called until SUCCEEDED, including the failed call
        mock_get_status.assert_any_call(run_id)
        self.assertEqual(mock_sleep.call_count, 2) # Sleep after RUNNING, sleep after API Error
        mock_sleep.assert_any_call(check_interval)
        self.assertGreaterEqual(mock_time.call_count, 4) # Initial time, plus checks before each status call


    @patch('requests.get')
    def test_fetch_dataset_success(self, mock_requests_get):
        """Test la récupération réussie du dataset"""
        dataset_id = "test_dataset_456"
        dataset_items = [{"item1": "data"}, {"item2": "more data"}]
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = dataset_items
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        result = self.client.fetch_dataset(dataset_id)

        self.assertEqual(result, dataset_items)
        mock_requests_get.assert_called_once_with(
            f'{self.base_url}/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
        )
        mock_response.raise_for_status.assert_called_once()

    def test_fetch_dataset_no_dataset_id(self):
        """Test la récupération avec un dataset ID vide"""
        result = self.client.fetch_dataset("")
        self.assertEqual(result, [])

    @patch('requests.get')
    def test_fetch_dataset_api_error(self, mock_requests_get):
        """Test l'erreur API lors de la récupération du dataset"""
        dataset_id = "test_dataset_456"
        mock_requests_get.side_effect = requests.exceptions.RequestException("Connection error")

        with self.assertRaisesRegex(Exception, f"Erreur de requête API lors de la récupération du dataset {dataset_id}: Connection error"):
            self.client.fetch_dataset(dataset_id)

        mock_requests_get.assert_called_once()

    @patch('requests.get')
    def test_fetch_dataset_api_non_200(self, mock_requests_get):
        """Test la réponse API non 2xx lors de la récupération du dataset"""
        dataset_id = "test_dataset_456"
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Internal Server Error")
        mock_requests_get.return_value = mock_response

        with self.assertRaisesRegex(Exception, f"Erreur de requête API lors de la récupération du dataset {dataset_id}: Internal Server Error"):
            self.client.fetch_dataset(dataset_id)

        mock_requests_get.assert_called_once()
        mock_response.raise_for_status.assert_called_once()

    @patch('requests.get')
    @patch('builtins.print') # Mock print to capture warning
    def test_fetch_dataset_invalid_json(self, mock_print, mock_requests_get):
        """Test la réponse API non JSON lors de la récupération du dataset"""
        dataset_id = "test_dataset_456"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        result = self.client.fetch_dataset(dataset_id)

        self.assertEqual(result, [])
        mock_requests_get.assert_called_once_with(
            f'{self.base_url}/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
        )
        mock_response.raise_for_status.assert_called_once()
        mock_response.json.assert_called_once()
        mock_print.assert_called_once_with(f"⚠️ Avertissement: La réponse de l'API pour le dataset {dataset_id} n'est pas du JSON valide.")


# --- Exécution des tests ---
if __name__ == '__main__':
    # Run tests and capture output for the report
    import sys
    from io import StringIO

    old_stdout = sys.stdout
    sys.stdout = captured_output = StringIO()

    print("Starting tests...")
    try:
        # Use TestLoader to load all tests from the class and run with TextTestRunner
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromTestCase(TestApifyAPIClient)
        runner = unittest.TextTestRunner(stream=captured_output, verbosity=2)
        result = runner.run(suite)
    finally:
        sys.stdout = old_stdout # Restore stdout

    # --- Génération du Rapport ---

    report = "\n\n"
    report += "=========================================\n"
    report += "         RAPPORT DE TEST APIfyAPIClient\n"
    report += "=========================================\n"
    report += "\n"
    report += "Outil de test: unittest (Python Standard Library)\n"
    report += "Méthodologie: Tests unitaires avec mocking des dépendances externes (requests, time)\n"
    report += "\n"
    report += "Résumé des tests :\n"
    report += f"- Nombre total de tests exécutés : {result.testsRun}\n"
    report += f"- Succès : {result.testsRun - len(result.errors) - len(result.failures)}\n"
    report += f"- Échecs : {len(result.failures)}\n"
    report += f"- Erreurs (exceptions imprévues) : {len(result.errors)}\n"
    report += "\n"

    if result.wasSuccessful():
        report += "Conclusion : TOUS LES TESTS ONT RÉUSSI.\n"
        report += "La classe ApifyAPIClient semble fonctionner correctement selon les scénarios testés.\n"
    else:
        report += "Conclusion : CERTAINS TESTS ONT ÉCHOUÉ OU ONT RENCONTRÉ DES ERREURS.\n"
        report += "Des problèmes ont été détectés dans l'implémentation. Voir les détails ci-dessous.\n"

    report += "\nDétails de l'exécution des tests :\n"
    report += "-----------------------------------------\n"
    report += captured_output.getvalue()
    report += "-----------------------------------------\n"

    if result.failures:
        report += "\nDÉTAILS DES ÉCHECS :\n"
        report += "---------------------\n"
        for test, trace in result.failures:
            report += f"Test échoué : {test}\n"
            report += f"Trace :\n{trace}\n"
            report += "---------------------\n"

    if result.errors:
        report += "\nDÉTAILS DES ERREURS :\n"
        report += "---------------------\n"
        for test, trace in result.errors:
            report += f"Erreur dans le test : {test}\n"
            report += f"Trace :\n{trace}\n"
            report += "---------------------\n"

    print(report)
