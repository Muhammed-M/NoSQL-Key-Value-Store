"""
Replicated HTTP Server for the Key-Value Store.
Supports primary-secondary replication with leader election.
"""
import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from .kv_store import KeyValueStore


class ReplicatedKVRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, kv_store: KeyValueStore = None, replication_node=None, **kwargs):
        self.kv_store = kv_store
        self.replication_node = replication_node
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
            
            # Only primary handles reads (or redirect to primary)
            if self.replication_node and not self.replication_node.is_primary:
                if self.replication_node.primary_node:
                    primary_host, primary_port = self.replication_node.primary_node
                    self._send_response(307, {"redirect": f"http://{primary_host}:{primary_port}/get?key={key}"})
                else:
                    self._send_response(503, {"error": "No primary available"})
                return
            
            value = self.kv_store.get(key)
            if value is None:
                self._send_response(404, {"error": "Key not found"})
            else:
                self._send_response(200, {"key": key, "value": value})
        
        elif parsed_path.path == "/ping":
            self._send_response(200, {"status": "ok"})
        
        elif parsed_path.path == "/vote":
            self._handle_vote_request()
        
        elif parsed_path.path == "/heartbeat":
            self._handle_heartbeat()
        
        elif parsed_path.path == "/replicate":
            self._handle_replicate()
        
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
        elif parsed_path.path == "/vote":
            self._handle_vote_request()
        elif parsed_path.path == "/heartbeat":
            self._handle_heartbeat()
        elif parsed_path.path == "/replicate":
            self._handle_replicate()
        else:
            self._send_response(404, {"error": "Not found"})
    
    def _handle_set(self):
        """Handle Set operation."""
        # Only primary handles writes
        if self.replication_node and not self.replication_node.is_primary:
            if self.replication_node.primary_node:
                primary_host, primary_port = self.replication_node.primary_node
                self._send_response(307, {"redirect": f"http://{primary_host}:{primary_port}/set"})
            else:
                self._send_response(503, {"error": "No primary available"})
            return
        
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
            
            # Replicate to secondaries if primary
            if self.replication_node and self.replication_node.is_primary:
                self.replication_node.replicate_to_secondaries({
                    "op": "set",
                    "key": key,
                    "value": value
                })
            
            self._send_response(200, {"success": success})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_delete(self):
        """Handle Delete operation."""
        # Only primary handles writes
        if self.replication_node and not self.replication_node.is_primary:
            if self.replication_node.primary_node:
                primary_host, primary_port = self.replication_node.primary_node
                self._send_response(307, {"redirect": f"http://{primary_host}:{primary_port}/delete"})
            else:
                self._send_response(503, {"error": "No primary available"})
            return
        
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
            
            # Replicate to secondaries if primary
            if self.replication_node and self.replication_node.is_primary:
                self.replication_node.replicate_to_secondaries({
                    "op": "delete",
                    "key": key
                })
            
            self._send_response(200, {"success": success})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_bulk_set(self):
        """Handle BulkSet operation."""
        # Only primary handles writes
        if self.replication_node and not self.replication_node.is_primary:
            if self.replication_node.primary_node:
                primary_host, primary_port = self.replication_node.primary_node
                self._send_response(307, {"redirect": f"http://{primary_host}:{primary_port}/bulk_set"})
            else:
                self._send_response(503, {"error": "No primary available"})
            return
        
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            items = data.get("items", [])
            simulate_failure = data.get("simulate_failure", False)
            
            if not items or not isinstance(items, list):
                self._send_response(400, {"error": "Missing or invalid items"})
                return
            
            items_tuples = [(item["key"], item["value"]) for item in items]
            count = self.kv_store.bulk_set(items_tuples, simulate_failure=simulate_failure)
            
            # Replicate to secondaries if primary
            if self.replication_node and self.replication_node.is_primary:
                self.replication_node.replicate_to_secondaries({
                    "op": "bulk_set",
                    "items": [{"key": k, "value": v} for k, v in items_tuples]
                })
            
            self._send_response(200, {"count": count})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_vote_request(self):
        """Handle vote request for leader election."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            term = data.get("term")
            candidate_id = data.get("candidate_id")
            
            if self.replication_node:
                vote_granted = self.replication_node.handle_vote_request(term, candidate_id)
                self._send_response(200, {"vote_granted": vote_granted, "term": self.replication_node.term})
            else:
                self._send_response(500, {"error": "No replication node"})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_heartbeat(self):
        """Handle heartbeat from primary."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            term = data.get("term")
            primary_id = data.get("primary_id")
            primary_host = data.get("primary_host")
            primary_port = data.get("primary_port")
            
            if self.replication_node:
                with self.replication_node._lock:
                    if term >= self.replication_node.term:
                        self.replication_node.term = term
                        self.replication_node.is_primary = False
                        if primary_host and primary_port:
                            self.replication_node.primary_node = (primary_host, primary_port)
                        self.replication_node.last_heartbeat = time.time()
                
                self._send_response(200, {"status": "ok"})
            else:
                self._send_response(500, {"error": "No replication node"})
        except Exception as e:
            self._send_response(500, {"error": str(e)})
    
    def _handle_replicate(self):
        """Handle replication from primary."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body)
            
            if self.replication_node:
                self.replication_node.apply_operation(data)
                self._send_response(200, {"status": "ok"})
            else:
                self._send_response(500, {"error": "No replication node"})
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


def create_replicated_handler(kv_store: KeyValueStore, replication_node):
    """Factory function to create request handler with kv_store and replication."""
    def handler(*args, **kwargs):
        return ReplicatedKVRequestHandler(*args, kv_store=kv_store, replication_node=replication_node, **kwargs)
    return handler


class ReplicatedKVServer:
    def __init__(self, node_id: int, host: str, port: int, peers: List[Tuple[str, int]], 
                 data_dir: str = "data", debug: bool = False):
        """
        Initialize the Replicated KV Server.
        
        Args:
            node_id: Unique node identifier
            host: Server host
            port: Server port
            peers: List of (host, port) tuples for other nodes
            data_dir: Data directory
            debug: Debug mode
        """
        self.host = host
        self.port = port
        
        from .replication import ReplicationNode
        self.replication_node = ReplicationNode(
            node_id=node_id,
            host=host,
            port=port,
            peers=peers,
            data_dir=data_dir,
            debug=debug
        )
        
        self.kv_store = self.replication_node.kv_store
        self.server = None
        
        # Start health check thread
        self._health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self._health_check_thread.start()
    
    def _health_check_loop(self):
        """Periodically check primary health."""
        while True:
            time.sleep(1)
            self.replication_node.check_primary_health()
    
    def start(self):
        """Start the server."""
        handler = create_replicated_handler(self.kv_store, self.replication_node)
        self.server = HTTPServer((self.host, self.port), handler)
        print(f"Replicated KV Server (Node {self.replication_node.node_id}) started on http://{self.host}:{self.port}")
        print(f"  Primary: {self.replication_node.is_primary}")
        self.server.serve_forever()
    
    def stop(self):
        """Stop the server gracefully."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.replication_node.stop()
            self.kv_store.checkpoint()
            print("Replicated KV Server stopped")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python replicated_server.py <node_id> <port> <peer1_host:port> [peer2_host:port] ...")
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
    
    server = ReplicatedKVServer(
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

