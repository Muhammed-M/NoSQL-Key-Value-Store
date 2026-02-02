"""
Master-less HTTP Server for the Key-Value Store.
Supports quorum-based reads/writes and conflict resolution.
"""
import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from .masterless_replication import MasterlessNode


class MasterlessKVRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, kv_store=None, masterless_node=None, **kwargs):
        self.kv_store = kv_store
        self.masterless_node = masterless_node
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)
        
        if parsed_path.path == "/get":
            key = query_params.get("key", [None])[0]
            if not key:
                self._send_response(400, {"error": "Missing key parameter"})
                return
            
            result = self.masterless_node.get(key)
            if result is None:
                self._send_response(404, {"error": "Key not found"})
            else:
                value, clock = result
                self._send_response(200, {"key": key, "value": value, "clock": clock})
        
        elif parsed_path.path == "/replicate_get":
            key = query_params.get("key", [None])[0]
            if not key:
                self._send_response(400, {"error": "Missing key parameter"})
                return
            
            result = self.masterless_node.replicate_get(key)
            if result is None:
                self._send_response(404, {"error": "Key not found"})
            else:
                value, clock = result
                self._send_response(200, {"value": value, "clock": clock})
        
        else:
            self._send_response(404, {"error": "Not found"})
    
    def do_POST(self):
        """Handle POST requests."""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == "/set":
            self._handle_set()
        elif parsed_path.path == "/delete":
            self._handle_delete()
        elif parsed_path.path == "/bulk_set":
            self._handle_bulk_set()
        elif parsed_path.path == "/replicate_set":
            self._handle_replicate_set()
        elif parsed_path.path == "/gossip":
            self._handle_gossip()
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
            
            success = self.masterless_node.set(key, value, simulate_failure=simulate_failure)
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
            
            # For delete, we set value to None or use a tombstone
            success = self.masterless_node.set(key, "__DELETED__", simulate_failure=simulate_failure)
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
            
            # Set each item individually (could be optimized)
            success_count = 0
            for item in items:
                if self.masterless_node.set(item["key"], item["value"], simulate_failure):
                    success_count += 1
            
            self._send_response(200, {"count": success_count})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_replicate_set(self):
        """Handle replicated set from another node."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            key = data.get("key")
            value = data.get("value")
            clock = data.get("clock", {})
            
            success = self.masterless_node.replicate_set(key, value, clock)
            self._send_response(200, {"success": success})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_gossip(self):
        """Handle gossip from another node."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            node_id = data.get("node_id")
            clock = data.get("clock", {})
            value_clocks = data.get("value_clocks", {})
            
            self.masterless_node.handle_gossip(node_id, clock, value_clocks)
            self._send_response(200, {"status": "ok"})
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


def create_masterless_handler(kv_store, masterless_node):
    """Factory function to create request handler."""
    def handler(*args, **kwargs):
        return MasterlessKVRequestHandler(*args, kv_store=kv_store, masterless_node=masterless_node, **kwargs)
    return handler


class MasterlessKVServer:
    def __init__(self, node_id: int, host: str, port: int, peers: List[tuple], 
                 data_dir: str = "data", debug: bool = False, replication_factor: int = 3):
        """
        Initialize the Master-less KV Server.
        
        Args:
            node_id: Unique node identifier
            host: Server host
            port: Server port
            peers: List of (host, port) tuples for other nodes
            data_dir: Data directory
            debug: Debug mode
            replication_factor: Replication factor
        """
        self.host = host
        self.port = port
        
        self.masterless_node = MasterlessNode(
            node_id=node_id,
            host=host,
            port=port,
            peers=peers,
            data_dir=data_dir,
            debug=debug,
            replication_factor=replication_factor
        )
        
        self.kv_store = self.masterless_node.kv_store
        self.server = None
        
        # Start gossip
        self.masterless_node.start_gossip()
    
    def start(self):
        """Start the server."""
        handler = create_masterless_handler(self.kv_store, self.masterless_node)
        self.server = HTTPServer((self.host, self.port), handler)
        print(f"Master-less KV Server (Node {self.masterless_node.node_id}) started on http://{self.host}:{self.port}")
        self.server.serve_forever()
    
    def stop(self):
        """Stop the server gracefully."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.masterless_node.stop()
            self.kv_store.checkpoint()
            print("Master-less KV Server stopped")


if __name__ == "__main__":
    import sys
    import urllib.parse
    
    if len(sys.argv) < 4:
        print("Usage: python masterless_server.py <node_id> <port> <peer1_host:port> [peer2_host:port] ...")
        sys.exit(1)
    
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    peers = []
    
    for i in range(3, len(sys.argv)):
        peer_str = sys.argv[i]
        if ":" in peer_str:
            peer_host, peer_port = peer_str.split(":")
            peers.append((peer_host, int(peer_port)))
    
    debug = False
    if len(sys.argv) > 3 + len(peers):
        debug = sys.argv[3 + len(peers)].lower() == "true"
    
    server = MasterlessKVServer(
        node_id=node_id,
        host="localhost",
        port=port,
        peers=peers,
        debug=debug
    )
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

