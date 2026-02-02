# Quick Start Guide

## Running a Single Node

```bash
python server.py 8080
```

Then in another terminal:
```python
from client import KVClient
client = KVClient(port=8080)
client.set("hello", "world")
print(client.get("hello"))  # "world"
```

## Running Tests

```bash
# Basic tests
python tests.py

# Replication tests
python test_replication.py

# Benchmarks
python benchmarks.py
```

## Running a Replicated Cluster

### Primary-Secondary (3 nodes)

Option 1: Use the helper script
```bash
python run_cluster.py
```

Option 2: Manual start
```bash
# Terminal 1
python replicated_server.py 1 9001 localhost:9002 localhost:9003

# Terminal 2
python replicated_server.py 2 9002 localhost:9001 localhost:9003

# Terminal 3
python replicated_server.py 3 9003 localhost:9001 localhost:9002
```

### Master-less (3 nodes)

```bash
python run_cluster.py masterless
```

Or manually:
```bash
# Terminal 1
python masterless_server.py 1 9101 localhost:9102 localhost:9103

# Terminal 2
python masterless_server.py 2 9102 localhost:9101 localhost:9103

# Terminal 3
python masterless_server.py 3 9103 localhost:9101 localhost:9102
```

## Using Indexes

```python
from kv_store import KeyValueStore
from indexes import IndexedKVStore

# Create indexed store
kv_store = KeyValueStore(data_dir="indexed_data")
indexed_store = IndexedKVStore(kv_store, data_dir="indexed_data")

# Add documents
indexed_store.set("doc1", "Python is a programming language")
indexed_store.set("doc2", "Java is also a programming language")
indexed_store.set("doc3", "Databases store data")

# Full-text search
results = indexed_store.fulltext_search("programming")
print(results)  # ["doc1", "doc2"]

# Embedding search (semantic similarity)
results = indexed_store.embedding_search("code language", top_k=2)
print(results)  # [("doc1", 0.85), ("doc2", 0.82)]
```

## Debug Mode

Enable debug mode to simulate file system sync issues (1% chance):

```bash
python server.py 8080 true
```

This helps test durability and recovery.

## Project Structure

```
.
├── kv_store.py              # Core KV store with WAL
├── server.py                # Single-node HTTP server
├── client.py                # Client library
├── replication.py           # Primary-secondary replication
├── replicated_server.py     # Server with primary-secondary
├── masterless_replication.py # Master-less replication logic
├── masterless_server.py     # Server with master-less replication
├── indexes.py               # Full-text and embedding indexes
├── tests.py                 # Basic tests
├── test_replication.py      # Replication tests
├── benchmarks.py            # Performance benchmarks
├── run_cluster.py           # Helper to run clusters
├── README.md                # Full documentation
└── requirements.txt         # Dependencies (none required)
```

