import requests
class InstagramApiCollector:
    def __init__(self, access_token, instagram_account_id,auto_version = True):
        """
        Initialize the Instagram API collector with authentication credentials.
        
        Args:
            access_token (str): Facebook Graph API access token with Instagram permissions
            instagram_account_id (str): ID of the Instagram Business/Creator account
        """
        self.access_token = access_token
        self.instagram_account_id = instagram_account_id
        self.default_api_version = "v22.0"
        if auto_version:
            self.api_version =self._detect_latest_version()
        else:
            self.api_version = self.default_api_version
        self.base_url = f"https://graph.facebook.com/{self.api_version}"

        self.session = requests.Session()
        
    def _detect_latest_version(self):
        """
        Detect the latest version of API Facebook
        """
        try:
            response = self.session.get("https://developers.facebook.com/docs/graph-api/changelog")
   
            if response.status_code == 200:
                #find patterns like "v22.0" in HTML
                import re
                versions = re.findall(r'v\d+\.\d+', response.text)
                if versions:
                    return versions[0]  # the latest_version
        
            return self.default_api_version
        except Exception as e:
            print(f"Error while detecting version: {e}")
            return self.default_api_version
        
    def get_media_list(self, limit=25, after=None):
        """
        Fetch recent Instagram posts (media objects).
        
        Args:
            limit (int): Number of posts to retrieve
            after (str, optional): Pagination cursor
            
        Returns:
            dict: Contains 'data' list of media objects and pagination info
        """
        endpoint = f"{self.base_url}/{self.instagram_account_id}/media"
        
        params = {
            "access_token": self.access_token,
            "limit": limit,
            "fields": "id,caption,media_type,media_url,permalink,thumbnail_url,timestamp,username,like_count,comments_count"
        }
        
        if after:
            params["after"] = after
            
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def get_media_comments(self, media_id, limit=50, after=None):
        """
        Fetch comments for a specific Instagram post.
        
        Args:
            media_id (str): Instagram media ID
            limit (int): Number of comments to retrieve
            after (str, optional): Pagination cursor
            
        Returns:
            dict: Contains 'data' list of comments and pagination info
        """
        endpoint = f"{self.base_url}/{media_id}/comments"
        
        params = {
            "access_token": self.access_token,
            "limit": limit,
            "fields": "id,text,username,timestamp,like_count,replies"
        }
        
        if after:
            params["after"] = after
            
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def get_comment_replies(self, comment_id, limit=25):
        """
        Fetch replies to a specific comment.
        
        Args:
            comment_id (str): Comment ID
            limit (int): Number of replies to retrieve
            
        Returns:
            dict: Contains 'data' list of replies
        """
        endpoint = f"{self.base_url}/{comment_id}/replies"
        
        params = {
            "access_token": self.access_token,
            "limit": limit,
            "fields": "id,text,username,timestamp,like_count"
        }
            
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        
        return response.json()
    
   
    