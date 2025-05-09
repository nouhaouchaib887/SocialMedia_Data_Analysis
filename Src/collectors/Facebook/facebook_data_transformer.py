"""
Facebook data transformer module with extensive debugging.
Handles parsing and transforming Facebook XML data into structured JSON format.
"""
from bs4 import BeautifulSoup
from datetime import datetime
import re
import traceback
import sys
from typing import Dict, List, Any, Optional, Union

class FacebookDataTransformer:
    """Transforms Facebook XML data into structured JSON format."""

    def unix_to_datetime(self, unix_timestamp: Union[int, str]) -> str:
        """Convert a Unix timestamp to an ISO format datetime string."""
        # Handle empty or non-numeric values
        if not unix_timestamp:
            return ""
            
        try:
            timestamp = int(unix_timestamp)
            return datetime.fromtimestamp(timestamp).isoformat()
        except (ValueError, TypeError):
            return ""
    
    def process_xml_data(self, xml_data: str) -> Dict[str, Any]:
        """Process the XML data and extract post, comments, pages, and users data."""
        try:
            print("Starting to process XML data")
            
            # Parse the XML data
            soup = BeautifulSoup(xml_data, 'xml')
            
            # Get the main container element
            container = soup.find("com.genesyslab.mcr.facebook.monitor.ResultsStreamWithComments")
            if not container:
                print("No container element found")
                return {}
                
            # Get the main elements
            stream_element = container.find("stream")
            comments_element = container.find("comments")
            pages_element = container.find("pages")
            users_element = container.find("users")
            
            # If stream element is missing, return empty data
            if not stream_element:
                print("No stream element found")
                return {}
            
            print("Building users dictionary")
            # Build dictionaries for users and pages
            users_dict = self.build_users_dict(users_element) if users_element else {}
            
            print("Building pages dictionary")
            pages_dict = self.build_pages_dict(pages_element) if pages_element else {}
            print(f"Pages dict type: {type(pages_dict)}")
            
            print("Extracting post data")
            # Extract post data
            post_data = self.extract_post_data(stream_element)
            print(f"Post data source_id: {post_data.get('source_id', 'None')}")
            
            # Add page information to post data - DEBUG VERSION
            print("Adding page information to post data")
            post_data["page_id"] = post_data.get("source_id", "")
            print(f"Page ID: {post_data['page_id']}")
            print(f"Pages dict keys: {list(pages_dict.keys())}")
            
            if post_data["page_id"] in pages_dict:
                print(f"Found page ID in pages_dict")
                page_info = pages_dict[post_data["page_id"]]
                print(f"Page info type: {type(page_info)}")
                print(f"Page info value: {page_info}")
                
                if isinstance(page_info, dict):
                    print("Page info is a dictionary")
                    post_data["page_name"] = page_info.get("name", "")
                else:
                    print("Page info is NOT a dictionary")
                    post_data["page_name"] = str(page_info)
            else:
                print("Page ID not found in pages_dict")
                post_data["page_name"] = ""
            
            print("Extracting comments data")
            # Extract comments data
            comments_data = {}
            if comments_element:
                comments_data = self.extract_comments_data(comments_element)
            
            # Add users to the final structure
            post_data["users"] = users_dict
            post_data["comments"] = comments_data
            
            print("XML processing completed successfully")
            return post_data
            
        except Exception as e:
            print(f"Error in process_xml_data: {str(e)}")
            print(f"Error type: {type(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def extract_post_data(self, stream_element) -> Dict[str, Any]:
        """Extract post data from the XML stream element."""
        try:
            post_data = {
                "platform": "facebook",
                "post_id": self._get_text(stream_element, "post__id"),
                "source_id": self._get_text(stream_element, "source__id"),
                "actor_id": self._get_text(stream_element, "actor__id"),
                "created_time": self.unix_to_datetime(self._get_text(stream_element, "created__time")),
                "updated_time": self.unix_to_datetime(self._get_text(stream_element, "updated__time")),
                "message": self._get_text(stream_element, "message"),
                "permalink": self._get_text(stream_element, "permalink"),
            }
            
            # Extract like info
            like_info = stream_element.find("like__info")
            if like_info is not None:
                post_data["can_like"] = self._parse_bool(self._get_text(like_info, "can__like"))
                post_data["like_count"] = int(self._get_text(like_info, "like__count") or 0)
            else:
                post_data["can_like"] = False
                post_data["like_count"] = 0
            
            # Extract comment info
            comment_info = stream_element.find("comment__info")
            if comment_info is not None:
                post_data["can_comment"] = self._parse_bool(self._get_text(comment_info, "can__comment"))
                post_data["comments_count"] = int(self._get_text(comment_info, "comment__count") or 0)
            else:
                post_data["can_comment"] = False
                post_data["comments_count"] = 0
            
            # Extract share info
            share_info = stream_element.find("share__info")
            if share_info is not None:
                post_data["can_share"] = self._parse_bool(self._get_text(share_info, "can__share"))
                post_data["shares"] = int(self._get_text(share_info, "share__count") or 0)
            else:
                post_data["can_share"] = False
                post_data["shares"] = 0
            
            # Extract media info from attachment
            attachment = stream_element.find("attachment")
            if attachment is not None:
                post_data["media_type"] = self._determine_media_type(attachment)
                post_data["media_url"] = self._extract_media_url(attachment)
                post_data["thumbnail_url"] = self._extract_thumbnail_url(attachment)
                post_data["caption"] = self._get_text(attachment, "caption")
                post_data["description"] = self._get_text(attachment, "description")
            else:
                post_data["media_type"] = "status"
                post_data["media_url"] = ""
                post_data["thumbnail_url"] = ""
                post_data["caption"] = ""
                post_data["description"] = ""
            
            # Extract hashtags
            post_data["hashtags"] = self._extract_hashtags(post_data["message"])
            
            return post_data
        except Exception as e:
            print(f"Error in extract_post_data: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def extract_comments_data(self, comments_element) -> Dict[str, Dict[str, Any]]:
        """Extract comments data from the XML comments element."""
        try:
            comments_dict = {}
            
            for entry in comments_element.find_all("entry"):
                string_element = entry.find("string")
                comment_element = entry.find("com.genesyslab.mcr.facebook.fql.Comment")
                
                if string_element is not None and comment_element is not None:
                    comment_id = string_element.text
                    parent_id = self._get_text(comment_element, "parent__id")
                    
                    # Convert parent_id to None if it's "0" (top-level comment)
                    if parent_id == "0":
                        parent_id = None
                    
                    comments_dict[comment_id] = {
                        "comment_id": comment_id,
                        "post_id": self._get_text(comment_element, "post__id"),
                        "parent_id": parent_id,
                        "message": self._get_text(comment_element, "text"),
                        "created_time": self.unix_to_datetime(self._get_text(comment_element, "time")),
                        "can_remove": self._parse_bool(self._get_text(comment_element, "can__remove")),
                        "can_comment": self._parse_bool(self._get_text(comment_element, "can__comment")),
                        "can_like": self._parse_bool(self._get_text(comment_element, "can__like")),
                        "from_id": self._get_text(comment_element, "fromid"),
                        "likes": int(self._get_text(comment_element, "likes") or 0),
                        "comments_count": int(self._get_text(comment_element, "comment__count") or 0),
                    }
            
            return comments_dict
        except Exception as e:
            print(f"Error in extract_comments_data: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def build_users_dict(self, users_element) -> Dict[str, Dict[str, Any]]:
        """Build a dictionary of users from the XML users element."""
        try:
            users_dict = {}
            
            if users_element is None:
                return users_dict
                
            for entry in users_element.find_all("entry"):
                string_element = entry.find("string")
                user_element = entry.find("com.genesyslab.mcr.facebook.fql.User")
                
                if string_element is not None and user_element is not None:
                    user_id = string_element.text
                    users_dict[user_id] = {
                        "user_id": user_id,
                        "can_post": self._parse_bool(self._get_text(user_element, "can__post")),
                        "first_name": self._get_text(user_element, "first__name"),
                        "last_name": self._get_text(user_element, "last__name"),
                        "name": self._get_text(user_element, "name"),
                        "profile_url": self._get_text(user_element, "profile__url"),
                        "pic_small": self._get_text(user_element, "pic__small"),
                    }
            
            return users_dict
        except Exception as e:
            print(f"Error in build_users_dict: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def build_pages_dict(self, pages_element) -> Dict[str, Dict[str, Any]]:
        """Build a dictionary of pages from the XML pages element."""
        try:
            pages_dict = {}
            
            if pages_element is None:
                print("Pages element is None, returning empty dict")
                return pages_dict
                
            print(f"Found {len(pages_element.find_all('entry'))} entries in pages_element")
            for entry in pages_element.find_all("entry"):
                string_element = entry.find("string")
                page_element = entry.find("com.genesyslab.mcr.facebook.fql.Page")
                
                print(f"Processing entry: string_element={string_element is not None}, page_element={page_element is not None}")
                
                if string_element is not None and page_element is not None:
                    page_id = string_element.text
                    print(f"Found page_id: {page_id}")
                    
                    # Debug page element content
                    print(f"Page element content: {page_element}")
                    
                    page_dict = {
                        "page_id": self._get_text(page_element, "page__id"),
                        "name": self._get_text(page_element, "name"),
                        "page_url": self._get_text(page_element, "page__url"),
                        "pic_small": self._get_text(page_element, "pic__small"),
                    }
                    print(f"Created page dict: {page_dict}")
                    
                    pages_dict[page_id] = page_dict
            
            print(f"Final pages_dict: {pages_dict}")
            return pages_dict
        except Exception as e:
            print(f"Error in build_pages_dict: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def _determine_media_type(self, attachment_element) -> str:
        """Determine the media type from the attachment element."""
        try:
            # Check for fb_object_type first
            fb_object_type = self._get_text(attachment_element, "fb__object__type")
            if fb_object_type:
                if fb_object_type.lower() == "video":
                    return "video"
                elif fb_object_type.lower() == "photo":
                    return "photo"
                elif fb_object_type.lower() == "link":
                    return "link"
            
            # Check if there's a media element with type attribute
            media = attachment_element.find("media")
            if media is not None:
                # First look for direct media type element
                if media.find("type") is not None:
                    media_type = media.find("type").text
                    if "video" in media_type.lower():
                        return "video"
                    elif "photo" in media_type.lower():
                        return "photo"
                
                # Then check collection of media items
                media_items = media.find_all("type")
                for item in media_items:
                    if item and item.text:
                        if "video" in item.text.lower():
                            return "video"
                        elif "photo" in item.text.lower():
                            return "photo"
            
            # Check for icon that might indicate media type
            icon_url = self._get_text(attachment_element, "icon")
            if icon_url and "video" in icon_url.lower():
                return "video"
            elif icon_url and "photo" in icon_url.lower():
                return "photo"
            
            return "status"  # Default type
        except Exception as e:
            print(f"Error in _determine_media_type: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def _extract_media_url(self, attachment_element) -> str:
        """Extract the media URL from the attachment element."""
        try:
            # First try the direct href attribute
            href = self._get_text(attachment_element, "href")
            if href:
                return href
                
            # If no direct href, check in media elements
            media = attachment_element.find("media")
            if media is not None:
                # Look for any href in media
                media_href = media.find("href")
                if media_href is not None and media_href.text:
                    return media_href.text
                    
                # If there are nested media elements, check each
                for item in media.find_all("com.genesyslab.mcr.facebook.fql.Stream_-Attachment_-Media"):
                    item_href = item.find("href")
                    if item_href is not None and item_href.text:
                        return item_href.text
            
            return ""
        except Exception as e:
            print(f"Error in _extract_media_url: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def _extract_thumbnail_url(self, attachment_element) -> str:
        """Extract the thumbnail URL from the attachment element."""
        try:
            # First check for direct src attribute
            src = self._get_text(attachment_element, "src")
            if src:
                return src
                
            # If no direct src, check in media elements
            media = attachment_element.find("media")
            if media is not None:
                # Look for any src in media
                media_src = media.find("src")
                if media_src is not None and media_src.text:
                    return media_src.text
                    
                # If there are nested media elements, check each
                for item in media.find_all("com.genesyslab.mcr.facebook.fql.Stream_-Attachment_-Media"):
                    item_src = item.find("src")
                    if item_src is not None and item_src.text:
                        return item_src.text
            
            return ""
        except Exception as e:
            print(f"Error in _extract_thumbnail_url: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def _extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from the text."""
        try:
            if not text:
                return []
            return re.findall(r"#(\w+)", text)
        except Exception as e:
            print(f"Error in _extract_hashtags: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def _get_text(self, element, tag_name: str) -> str:
        """Safely get text from an XML element."""
        try:
            if element is None:
                return ""
            tag = element.find(tag_name)
            if tag is not None and tag.text is not None:
                return tag.text
            return ""
        except Exception as e:
            print(f"Error in _get_text for tag {tag_name}: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise

    def _parse_bool(self, value: str) -> bool:
        """Parse a string to boolean."""
        try:
            if not value:
                return False
            return value.lower() == "true"
        except Exception as e:
            print(f"Error in _parse_bool for value {value}: {str(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            raise