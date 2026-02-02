"""
Master-less replication system for the Key-Value Store.
Uses quorum-based reads/writes and vector clocks for conflict resolution.
"""
import json
import os
import threading
import time
import urllib.request
import urllib.parse
from typing import Optional, Dict, List, Tuple
from collections import defaultdict
from kv_store import KeyValueStore


class VectorClock:
    """Vector clock for tracking causality."""
    
    def __init__(self, node_id: int):
        """
        Initialize vector clock.
        
        Args:
            node_id: Node identifier
        """
        self.node_id = node_id
        self.clock: Dict[int, int] = defaultdict(int)
        self.clock[node_id] = 0
    
    def tick(self):
        """Increment own clock."""
        self.clock[self.node_id] += 1
    
    def update(self, other_clock: Dict[int, int]):
        """Update clock with another clock (merge)."""
        for node_id, value in other_clock.items():
            self.clock[node_id] = max(self.clock[node_id], value)
        self.clock[self.node_id] += 1
    
    def compare(self, other_clock: Dict[int, int]) -> int:
        """
        Compare with another clock.
        
        Returns:
            -1 if this < other, 0 if concurrent, 1 if this > other
        """
        less = False
        greater = False
        
        all_nodes = set(self.clock.keys()) | set(other_clock.keys())
        
        for node_id in all_nodes:
            this_val = self.clock.get(node_id, 0)
            other_val = other_clock.get(node_id, 0)
            
            if this_val < other_val:
                less = True
            elif this_val > other_val:
                greater = True
        
        if less and not greater:
            return -1
        elif greater and not less:
            return 1
        else:
            return 0  # Concurrent
    
    def to_dict(self) -> Dict[int, int]:
        """Convert to dictionary."""
        return dict(self.clock)
    
    def from_dict(self, data: Dict[int, int]):
        """Load from dictionary."""
        self.clock = defaultdict(int, data)


class MasterlessNode:
    """Master-less replication node."""
    
    def __init__(self, node_id: int, host: str, port: int, peers: List[Tuple[str, int]], 
                 data_dir: str = "data", debug: bool = False, replication_factor: int = 3):
        """
        Initialize a master-less node.
        
        Args:
            node_id: Unique identifier for this node
            host: Host address
            port: Port number
            peers: List of (host, port) tuples for other nodes
            data_dir: Data directory
            debug: Debug mode
            replication_factor: Number of replicas (including self)
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.data_dir = data_dir
        self.debug = debug
        self.replication_factor = replication_factor
        
        self.kv_store = KeyValueStore(data_dir=f"{data_dir}_node_{node_id}", debug=debug)
        self.vector_clock = VectorClock(node_id)
        self.value_clocks: Dict[str, Dict[int, int]] = {}  # key -> vector clock
        self.value_versions: Dict[str, Tuple[str, Dict[int, int]]] = {}  # key -> (value, clock)
        
        self._lock = threading.RLock()
        self._gossip_thread = None
        self._running = False
        
        # Load persisted vector clocks
        self._load_clocks()
    
    def _load_clocks(self):
        """Load vector clocks from disk."""
        clocks_file = os.path.join(f"{self.data_dir}_node_{self.node_id}", "clocks.json")
        if os.path.exists(clocks_file):
            try:
                with open(clocks_file, "r") as f:
                    data = json.load(f)
                    self.value_clocks = {k: {int(nid): v for nid, v in vc.items()} 
                                       for k, vc in data.get("value_clocks", {}).items()}
                    clock_data = data.get("vector_clock", {})
                    self.vector_clock.from_dict({int(nid): v for nid, v in clock_data.items()})
            except (json.JSONDecodeError, IOError):
                pass
    
    def _save_clocks(self):
        """Save vector clocks to disk."""
        clocks_file = os.path.join(f"{self.data_dir}_node_{self.node_id}", "clocks.json")
        os.makedirs(os.path.dirname(clocks_file), exist_ok=True)
        with open(clocks_file, "w") as f:
            json.dump({
                "vector_clock": self.vector_clock.to_dict(),
                "value_clocks": self.value_clocks
            }, f)
            f.flush()
            os.fsync(f.fileno())
    
    def _get_replica_nodes(self, key: str) -> List[Tuple[str, int]]:
        """Get replica nodes for a key using consistent hashing."""
        # Simple hash-based replica selection
        all_nodes = [(self.host, self.port)] + self.peers
        key_hash = hash(key) % len(all_nodes)
        replicas = []
        for i in range(self.replication_factor):
            idx = (key_hash + i) % len(all_nodes)
            replicas.append(all_nodes[idx])
        return replicas
    
    def set(self, key: str, value: str, simulate_failure: bool = False) -> bool:
        """
        Set a key-value pair with quorum-based replication.
        
        Args:
            key: The key
            value: The value
            simulate_failure: Simulate failure
        
        Returns:
            True if quorum achieved
        """
        with self._lock:
            # Update vector clock
            self.vector_clock.tick()
            clock = self.vector_clock.to_dict()
            
            # Store locally
            self.kv_store.set(key, value, simulate_failure=simulate_failure)
            self.value_clocks[key] = clock.copy()
            self.value_versions[key] = (value, clock.copy())
            
            # Replicate to other nodes
            replica_nodes = self._get_replica_nodes(key)
            success_count = 1  # Count ourselves
            
            for peer_host, peer_port in replica_nodes:
                if (peer_host, peer_port) == (self.host, self.port):
                    continue
                
                try:
                    url = f"http://{peer_host}:{peer_port}/replicate_set"
                    data = json.dumps({
                        "key": key,
                        "value": value,
                        "clock": clock
                    }).encode("utf-8")
                    
                    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                    with urllib.request.urlopen(req, timeout=1.0) as response:
                        success_count += 1
                except:
                    pass
            
            # Quorum: need majority (replication_factor // 2 + 1)
            quorum_size = self.replication_factor // 2 + 1
            success = success_count >= quorum_size
            
            if success:
                self._save_clocks()
            
            return success
    
    def get(self, key: str, read_quorum: Optional[int] = None) -> Optional[Tuple[str, Dict[int, int]]]:
        """
        Get value with quorum-based read.
        
        Args:
            key: The key
            read_quorum: Number of nodes to read from (default: replication_factor // 2 + 1)
        
        Returns:
            Tuple of (value, clock) or None
        """
        if read_quorum is None:
            read_quorum = self.replication_factor // 2 + 1
        
        # Read from local store first
        local_value = self.kv_store.get(key)
        local_clock = self.value_clocks.get(key, {})
        
        # Read from replicas
        replica_nodes = self._get_replica_nodes(key)
        values = []
        
        if local_value:
            values.append((local_value, local_clock))
        
        for peer_host, peer_port in replica_nodes:
            if (peer_host, peer_port) == (self.host, self.port):
                continue
            
            try:
                url = f"http://{peer_host}:{peer_port}/replicate_get?key={urllib.parse.quote(key)}"
                with urllib.request.urlopen(url, timeout=1.0) as response:
                    data = json.loads(response.read().decode("utf-8"))
                    if data.get("value") is not None:
                        values.append((data["value"], data.get("clock", {})))
            except:
                pass
        
        if len(values) < read_quorum:
            return None
        
        # Resolve conflicts: pick value with highest vector clock
        best_value = None
        best_clock = {}
        
        for value, clock in values:
            if not best_clock:
                best_value = value
                best_clock = clock
            else:
                comparison = self.vector_clock.compare(clock)
                if comparison < 0:  # clock > best_clock
                    best_value = value
                    best_clock = clock
        
        return (best_value, best_clock) if best_value else None
    
    def replicate_set(self, key: str, value: str, clock: Dict[int, int]):
        """Handle replicated set from another node."""
        with self._lock:
            # Check if this update is newer or concurrent
            existing_clock = self.value_clocks.get(key, {})
            
            # Update vector clock
            self.vector_clock.update(clock)
            
            # Resolve conflicts
            comparison = self.vector_clock.compare(existing_clock)
            if comparison >= 0 or not existing_clock:
                # Accept this update
                self.kv_store.set(key, value)
                self.value_clocks[key] = clock.copy()
                self.value_versions[key] = (value, clock.copy())
                self._save_clocks()
                return True
            else:
                # Keep existing (it's newer)
                return False
    
    def replicate_get(self, key: str) -> Optional[Tuple[str, Dict[int, int]]]:
        """Handle replicated get from another node."""
        value = self.kv_store.get(key)
        clock = self.value_clocks.get(key, {})
        return (value, clock) if value else None
    
    def gossip(self):
        """Gossip protocol to sync state with peers."""
        if not self._running:
            return
        
        for peer_host, peer_port in self.peers:
            try:
                # Send our vector clock
                url = f"http://{peer_host}:{peer_port}/gossip"
                data = json.dumps({
                    "node_id": self.node_id,
                    "clock": self.vector_clock.to_dict(),
                    "value_clocks": {k: v for k, v in self.value_clocks.items()}
                }).encode("utf-8")
                
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=1.0) as response:
                    # Handle response if needed
                    pass
            except:
                pass
    
    def handle_gossip(self, node_id: int, clock: Dict[int, int], value_clocks: Dict[str, Dict[int, int]]):
        """Handle gossip from another node."""
        with self._lock:
            # Update our vector clock
            self.vector_clock.update(clock)
            
            # Check for missing or outdated values
            for key, peer_clock in value_clocks.items():
                local_clock = self.value_clocks.get(key, {})
                comparison = self.vector_clock.compare(peer_clock)
                
                if comparison < 0:  # Peer has newer version
                    # Request value from peer
                    pass  # In full implementation, would fetch value
    
    def start_gossip(self):
        """Start gossip thread."""
        if self._gossip_thread and self._gossip_thread.is_alive():
            return
        
        self._running = True
        self._gossip_thread = threading.Thread(target=self._gossip_loop, daemon=True)
        self._gossip_thread.start()
    
    def _gossip_loop(self):
        """Gossip loop."""
        while self._running:
            time.sleep(2)  # Gossip every 2 seconds
            self.gossip()
    
    def stop(self):
        """Stop the node."""
        self._running = False
        if self._gossip_thread:
            self._gossip_thread.join(timeout=1)
        self._save_clocks()

