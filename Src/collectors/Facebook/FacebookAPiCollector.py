import requests


class FacebookAPiCollector:
    def __init__(self, access_token,page_id, auto_version= True):
        """
            Initialize the Facebook API Collector

            Args:

                access_token (str): Facebook API Access Token with page permissions
                page_id(str): ID of Facebook page

        """
        self.access_token = access_token
        self.page_id = page_id
        self.session =requests.Session()
        self.default_api_version = "v22.0"
        if auto_version:
            self.api_version =self._detect_latest_version()
        else:
            self.api_version = self.default_api_version
        self.base_url = f"https://graph.facebook.com/{self.api_version}"

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

        def get_page_posts(self, limit =10, since =None, until = None):
            """
            Fetch recent posts from facebook page

            args:

                limit (int): Number of posts to retrieve
                since (str,optional): Start date "YYYY-MM-DD"
                until (str, optional) : End Date (YYYY-MM-DD)

            Returns:
                list: Collection of posts
            """
            endpoint = f"{self.base_url}/{self.page_id}/posts"

            params = {
                "access_token": self.access_token,
                "limit": limit,
                "fields" :(
                    "id,message,created_time,shares,comments.summary(true),reactions.summary(true),"
                    "reactions.type(LIKE).limit(0).summary(total_count).as(like_count),"
                    "reactions.type(LOVE).limit(0).summary(total_count).as(love_count),"
                    "reactions.type(WOW).limit(0).summary(total_count).as(wow_count),"
                    "reactions.type(HAHA).limit(0).summary(total_count).as(haha_count),"
                    "reactions.type(SAD).limit(0).summary(total_count).as(sad_count),"
                    "reactions.type(ANGRY).limit(0).summary(total_count).as(angry_count),"
                    "insights.metric(post_impressions)"
                    )
            }
            if since :
                params["since"] = since
            if until:
                parems["until"] = until

            response = self.session.get(endpoint ,params = params)
            response.raise_for_status()
            return response.json().get("data",[])
        def get_posts_comments(self,post_id, limit=25):
            """
            Fetch comments for a specific post

            args:
                post_id (str) : Facebook Post ID
                limit (int): Number of comments to retrieve
            """

