"""
Client for the Key-Value Store.
Provides a simple interface to interact with the KV store server.
"""
import json
import urllib.request
import urllib.parse
from typing import Optional, List, Tuple


class KVClient:
    def __init__(self, host: str = "localhost", port: int = 8080):
        """
        Initialize the KV Client.
        
        Args:
            host: Server host
            port: Server port
        """
        self.base_url = f"http://{host}:{port}"
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value for a key.
        
        Args:
            key: The key
        
        Returns:
            The value if exists, None otherwise
        """
        try:
            url = f"{self.base_url}/get?key={urllib.parse.quote(key)}"
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode("utf-8"))
                return data.get("value")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise
        except Exception as e:
            raise ConnectionError(f"Failed to connect to server: {e}")
    
    def set(self, key: str, value: str, simulate_failure: bool = False) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: The key
            value: The value
            simulate_failure: If True, simulate file system sync failure (debug mode)
        
        Returns:
            True if successful
        """
        try:
            url = f"{self.base_url}/set"
            data = json.dumps({
                "key": key,
                "value": value,
                "simulate_failure": simulate_failure
            }).encode("utf-8")
            
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result.get("success", False)
        except Exception as e:
            raise ConnectionError(f"Failed to set key: {e}")
    
    def delete(self, key: str, simulate_failure: bool = False) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            simulate_failure: If True, simulate file system sync failure (debug mode)
        
        Returns:
            True if key existed and was deleted, False otherwise
        """
        try:
            url = f"{self.base_url}/delete"
            data = json.dumps({
                "key": key,
                "simulate_failure": simulate_failure
            }).encode("utf-8")
            
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result.get("success", False)
        except Exception as e:
            raise ConnectionError(f"Failed to delete key: {e}")
    
    def bulk_set(self, items: List[Tuple[str, str]], simulate_failure: bool = False) -> int:
        """
        Set multiple key-value pairs atomically.
        
        Args:
            items: List of (key, value) tuples
            simulate_failure: If True, simulate file system sync failure (debug mode)
        
        Returns:
            Number of items set
        """
        try:
            url = f"{self.base_url}/bulk_set"
            data = json.dumps({
                "items": [{"key": k, "value": v} for k, v in items],
                "simulate_failure": simulate_failure
            }).encode("utf-8")
            
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result.get("count", 0)
        except Exception as e:
            raise ConnectionError(f"Failed to bulk set: {e}")

