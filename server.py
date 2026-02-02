"""
HTTP Server for the Key-Value Store.
Provides REST API endpoints for Set, Get, Delete, and BulkSet operations.
"""
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from kv_store import KeyValueStore


class KVRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, kv_store: KeyValueStore = None, **kwargs):
        self.kv_store = kv_store
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests for retrieving values."""
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)
        
        if parsed_path.path == "/get":
            key = query_params.get("key", [None])[0]
            if not key:
                self._send_response(400, {"error": "Missing key parameter"})
                return
            
            value = self.kv_store.get(key)
            if value is None:
                self._send_response(404, {"error": "Key not found"})
            else:
                self._send_response(200, {"key": key, "value": value})
        else:
            self._send_response(404, {"error": "Not found"})
    
    def do_POST(self):
        """Handle POST requests for Set, Delete, and BulkSet operations."""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == "/set":
            self._handle_set()
        elif parsed_path.path == "/delete":
            self._handle_delete()
        elif parsed_path.path == "/bulk_set":
            self._handle_bulk_set()
        else:
            self._send_response(404, {"error": "Not found"})
    
    def _handle_set(self):
        """Handle Set operation."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            key = data.get("key")
            value = data.get("value")
            simulate_failure = data.get("simulate_failure", False)
            
            if not key or value is None:
                self._send_response(400, {"error": "Missing key or value"})
                return
            
            success = self.kv_store.set(key, value, simulate_failure=simulate_failure)
            self._send_response(200, {"success": success})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_delete(self):
        """Handle Delete operation."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            key = data.get("key")
            simulate_failure = data.get("simulate_failure", False)
            
            if not key:
                self._send_response(400, {"error": "Missing key"})
                return
            
            success = self.kv_store.delete(key, simulate_failure=simulate_failure)
            self._send_response(200, {"success": success})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_bulk_set(self):
        """Handle BulkSet operation."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            items = data.get("items", [])
            simulate_failure = data.get("simulate_failure", False)
            
            if not items or not isinstance(items, list):
                self._send_response(400, {"error": "Missing or invalid items"})
                return
            
            # Convert items to list of tuples
            items_tuples = [(item["key"], item["value"]) for item in items]
            
            count = self.kv_store.bulk_set(items_tuples, simulate_failure=simulate_failure)
            self._send_response(200, {"count": count})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _send_response(self, status_code: int, data: dict):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def create_handler(kv_store: KeyValueStore):
    """Factory function to create request handler with kv_store."""
    def handler(*args, **kwargs):
        return KVRequestHandler(*args, kv_store=kv_store, **kwargs)
    return handler


class KVServer:
    def __init__(self, host: str = "localhost", port: int = 8080, data_dir: str = "data", debug: bool = False):
        """
        Initialize the KV Server.
        
        Args:
            host: Server host
            port: Server port
            data_dir: Data directory for persistence
            debug: Enable debug mode (simulate file system sync issues)
        """
        self.host = host
        self.port = port
        self.kv_store = KeyValueStore(data_dir=data_dir, debug=debug)
        self.server = None
    
    def start(self):
        """Start the server."""
        handler = create_handler(self.kv_store)
        self.server = HTTPServer((self.host, self.port), handler)
        print(f"KV Server started on http://{self.host}:{self.port}")
        self.server.serve_forever()
    
    def stop(self):
        """Stop the server gracefully."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            # Create checkpoint before shutdown
            self.kv_store.checkpoint()
            print("KV Server stopped")


if __name__ == "__main__":
    import sys
    
    port = 8080
    debug = False
    
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    if len(sys.argv) > 2:
        debug = sys.argv[2].lower() == "true"
    
    server = KVServer(host="localhost", port=port, debug=debug)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

