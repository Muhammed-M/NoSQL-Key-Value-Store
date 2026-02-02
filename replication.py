"""
Replication system for the Key-Value Store.
Supports primary-secondary replication with leader election.
"""
import json
import os
import threading
import time
import urllib.request
import urllib.parse
from typing import Optional, Dict, List, Tuple
from kv_store import KeyValueStore


class ReplicationNode:
    def __init__(self, node_id: int, host: str, port: int, peers: List[Tuple[str, int]], 
                 data_dir: str = "data", debug: bool = False):
        """
        Initialize a replication node.
        
        Args:
            node_id: Unique identifier for this node
            host: Host address
            port: Port number
            peers: List of (host, port) tuples for other nodes
            data_dir: Data directory
            debug: Debug mode
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.data_dir = data_dir
        self.debug = debug
        
        self.kv_store = KeyValueStore(data_dir=f"{data_dir}_node_{node_id}", debug=debug)
        self.is_primary = False
        self.primary_node = None
        self.term = 0
        self.voted_for = None
        self.last_heartbeat = time.time()
        
        self._lock = threading.RLock()
        self._replication_thread = None
        self._election_thread = None
        self._running = False
        
        # Start as secondary, will elect primary if no primary exists
        self.start_election()
    
    def start_election(self):
        """Start leader election process."""
        with self._lock:
            self.term += 1
            self.voted_for = self.node_id
            self.is_primary = False
        
        # Check if we can become primary
        votes = 1  # Vote for ourselves
        for peer_host, peer_port in self.peers:
            try:
                url = f"http://{peer_host}:{peer_port}/vote"
                data = json.dumps({
                    "term": self.term,
                    "candidate_id": self.node_id
                }).encode("utf-8")
                
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=0.5) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    if result.get("vote_granted"):
                        votes += 1
            except:
                pass
        
        # Become primary if we have majority (2 out of 3)
        if votes >= 2:
            with self._lock:
                self.is_primary = True
                self.primary_node = (self.host, self.port)
            print(f"Node {self.node_id} became PRIMARY")
            self._start_replication()
        else:
            with self._lock:
                self.is_primary = False
            print(f"Node {self.node_id} remains SECONDARY")
    
    def handle_vote_request(self, term: int, candidate_id: int) -> bool:
        """Handle vote request from candidate."""
        with self._lock:
            if term > self.term:
                self.term = term
                self.voted_for = candidate_id
                self.is_primary = False
                return True
            elif term == self.term and self.voted_for is None:
                self.voted_for = candidate_id
                return True
            return False
    
    def replicate_to_secondaries(self, operation: dict):
        """Replicate operation to secondary nodes."""
        if not self.is_primary:
            return
        
        for peer_host, peer_port in self.peers:
            try:
                url = f"http://{peer_host}:{peer_port}/replicate"
                data = json.dumps(operation).encode("utf-8")
                
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=1.0) as response:
                    pass  # Success
            except:
                pass  # Secondary might be down
    
    def apply_operation(self, operation: dict):
        """Apply replicated operation."""
        op_type = operation.get("op")
        if op_type == "set":
            self.kv_store.set(operation["key"], operation["value"])
        elif op_type == "delete":
            self.kv_store.delete(operation["key"])
        elif op_type == "bulk_set":
            items = [(item["key"], item["value"]) for item in operation["items"]]
            self.kv_store.bulk_set(items)
    
    def check_primary_health(self):
        """Check if primary is still alive."""
        if self.is_primary:
            return True
        
        if self.primary_node:
            primary_host, primary_port = self.primary_node
            try:
                url = f"http://{primary_host}:{primary_port}/ping"
                with urllib.request.urlopen(url, timeout=0.5) as response:
                    self.last_heartbeat = time.time()
                    return True
            except:
                # Primary is down, start election
                if time.time() - self.last_heartbeat > 2.0:
                    self.start_election()
                return False
        
        return False
    
    def _start_replication(self):
        """Start replication thread."""
        if self._replication_thread and self._replication_thread.is_alive():
            return
        
        self._running = True
        self._replication_thread = threading.Thread(target=self._replication_loop, daemon=True)
        self._replication_thread.start()
    
    def _replication_loop(self):
        """Replication loop for primary."""
        while self._running and self.is_primary:
            time.sleep(0.1)
            # Heartbeat to secondaries
            for peer_host, peer_port in self.peers:
                try:
                    url = f"http://{peer_host}:{peer_port}/heartbeat"
                    data = json.dumps({
                        "term": self.term,
                        "primary_id": self.node_id,
                        "primary_host": self.host,
                        "primary_port": self.port
                    }).encode("utf-8")
                    
                    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                    with urllib.request.urlopen(req, timeout=0.5) as response:
                        pass
                except:
                    pass
    
    def stop(self):
        """Stop the node."""
        self._running = False
        if self._replication_thread:
            self._replication_thread.join(timeout=1)

