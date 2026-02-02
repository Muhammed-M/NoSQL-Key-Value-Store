"""
Core Key-Value Store with persistence.
Supports Set, Get, Delete, and BulkSet operations.
Uses WAL (Write-Ahead Log) for durability.
"""
import json
import os
import random
import threading
from typing import Optional, Dict, List, Tuple


class KeyValueStore:
    def __init__(self, data_dir: str = "data", debug: bool = False):
        """
        Initialize the key-value store.
        
        Args:
            data_dir: Directory to store data files
            debug: If True, simulate file system sync issues (1% chance)
        """
        self.data_dir = data_dir
        self.debug = debug
        self._data: Dict[str, str] = {}
        self._lock = threading.RLock()
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        self._data_file = os.path.join(data_dir, "data.json")
        self._wal_file = os.path.join(data_dir, "wal.log")
        
        # Load existing data
        self._load_data()
        self._replay_wal()
    
    def _load_data(self):
        """Load data from JSON file."""
        if os.path.exists(self._data_file):
            try:
                with open(self._data_file, "r") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._data = {}
    
    def _replay_wal(self):
        """Replay Write-Ahead Log to recover data."""
        if not os.path.exists(self._wal_file):
            return
        
        try:
            with open(self._wal_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        op = entry.get("op")
                        if op == "set":
                            self._data[entry["key"]] = entry["value"]
                        elif op == "delete":
                            self._data.pop(entry["key"], None)
                    except (json.JSONDecodeError, KeyError):
                        continue
        except IOError:
            pass
    
    def _append_wal(self, entry: dict):
        """Append entry to Write-Ahead Log (synchronous)."""
        with open(self._wal_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())  # Force sync to disk
    
    def _save(self, simulate_failure: bool = False):
        """
        Save data to disk.
        
        Args:
            simulate_failure: If True and debug=True, randomly skip save (1% chance)
        """
        if simulate_failure and self.debug:
            if random.random() < 0.01:
                return  # Simulate file system sync failure
        
        with open(self._data_file, "w") as f:
            json.dump(self._data, f)
            f.flush()
            os.fsync(f.fileno())  # Force sync to disk
    
    def set(self, key: str, value: str, simulate_failure: bool = False) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: The key
            value: The value
            simulate_failure: If True and debug=True, simulate save failure
        
        Returns:
            True if successful
        """
        with self._lock:
            # Write to WAL first (synchronous)
            self._append_wal({"op": "set", "key": key, "value": value})
            
            # Update in-memory data
            self._data[key] = value
            
            # Save to disk (may fail if simulate_failure=True and debug=True)
            self._save(simulate_failure)
            
            return True
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value for a key.
        
        Args:
            key: The key
        
        Returns:
            The value if exists, None otherwise
        """
        with self._lock:
            return self._data.get(key)
    
    def delete(self, key: str, simulate_failure: bool = False) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            simulate_failure: If True and debug=True, simulate save failure
        
        Returns:
            True if key existed and was deleted, False otherwise
        """
        with self._lock:
            if key not in self._data:
                return False
            
            # Write to WAL first (synchronous)
            self._append_wal({"op": "delete", "key": key})
            
            # Update in-memory data
            del self._data[key]
            
            # Save to disk (may fail if simulate_failure=True and debug=True)
            self._save(simulate_failure)
            
            return True
    
    def bulk_set(self, items: List[Tuple[str, str]], simulate_failure: bool = False) -> int:
        """
        Set multiple key-value pairs atomically.
        
        Args:
            items: List of (key, value) tuples
            simulate_failure: If True and debug=True, simulate save failure
        
        Returns:
            Number of items set
        """
        with self._lock:
            # Write all operations to WAL first (synchronous)
            for key, value in items:
                self._append_wal({"op": "set", "key": key, "value": value})
            
            # Update in-memory data
            for key, value in items:
                self._data[key] = value
            
            # Save to disk once (may fail if simulate_failure=True and debug=True)
            self._save(simulate_failure)
            
            return len(items)
    
    def clear_wal(self):
        """Clear the Write-Ahead Log (called after successful checkpoint)."""
        if os.path.exists(self._wal_file):
            os.remove(self._wal_file)
    
    def checkpoint(self):
        """Create a checkpoint by saving data and clearing WAL."""
        with self._lock:
            self._save()
            self.clear_wal()

