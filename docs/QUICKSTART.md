# Quick Start Guide

## Running a Single Node

```bash
python run_server.py 8080
```

Or directly:
```bash
python src/server.py 8080
```

Then in another terminal:
```python
import sys
import os
sys.path.insert(0, 'src')
from client import KVClient

client = KVClient(port=8080)
client.set("hello", "world")
print(client.get("hello"))  # "world"
```

## Running Tests

```bash
# Basic tests (using entry point)
python run_tests.py

# Or directly
python tests/tests.py

# Replication tests
python tests/test_replication.py

# Benchmarks (using entry point)
python run_benchmarks.py

# Or directly
python benchmarks/benchmarks.py
```

## Running a Replicated Cluster

### Primary-Secondary (3 nodes)

Option 1: Use the helper script (recommended)
```bash
python scripts/run_cluster.py
```

Option 2: Manual start
```bash
# Terminal 1
python src/replicated_server.py 1 9001 localhost:9002 localhost:9003

# Terminal 2
python src/replicated_server.py 2 9002 localhost:9001 localhost:9003

# Terminal 3
python src/replicated_server.py 3 9003 localhost:9001 localhost:9002
```

### Master-less (3 nodes)

```bash
python scripts/run_cluster.py masterless
```

Or manually:
```bash
# Terminal 1
python src/masterless_server.py 1 9101 localhost:9102 localhost:9103

# Terminal 2
python src/masterless_server.py 2 9102 localhost:9101 localhost:9103

# Terminal 3
python src/masterless_server.py 3 9103 localhost:9101 localhost:9102
```

## Using Indexes

```python
import sys
import os
sys.path.insert(0, 'src')
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
├── src/                    # Source code
│   ├── kv_store.py         # Core KV store with WAL
│   ├── server.py           # Single-node HTTP server
│   ├── client.py           # Client library
│   ├── replication.py      # Primary-secondary replication
│   ├── replicated_server.py # Server with primary-secondary
│   ├── masterless_replication.py # Master-less replication
│   ├── masterless_server.py # Server with master-less
│   └── indexes.py          # Full-text and embedding indexes
├── tests/                  # Test files
│   ├── tests.py            # Basic tests
│   └── test_replication.py # Replication tests
├── benchmarks/             # Benchmark files
│   └── benchmarks.py       # Performance benchmarks
├── scripts/                # Helper scripts
│   └── run_cluster.py      # Cluster runner
├── docs/                   # Documentation
│   ├── GITHUB_SETUP.md     # GitHub setup guide
│   └── QUICKSTART.md       # This file
├── run_server.py           # Entry point for single server
├── run_tests.py            # Entry point for tests
├── run_benchmarks.py       # Entry point for benchmarks
└── README.md               # Full documentation
```

