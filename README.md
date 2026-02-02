# Key-Value Store with Replication and Indexing

A distributed key-value store built in Python with support for replication, indexing, and ACID properties.

## Features

- **Core Operations**: Set, Get, Delete, BulkSet
- **Persistence**: Data persists across restarts using WAL (Write-Ahead Log)
- **Replication**: 
  - Primary-Secondary replication with leader election
  - Master-less replication with quorum-based reads/writes
- **Indexing**:
  - Full-text search index
  - Word embedding index for semantic search
- **ACID Properties**: Atomic operations, durability guarantees
- **Benchmarks**: Write throughput and durability tests

## Architecture

### Core Components

1. **kv_store.py**: Core key-value store with WAL-based persistence
2. **server.py**: HTTP server for single-node operation
3. **client.py**: Client library for interacting with the store
4. **replication.py**: Primary-secondary replication logic
5. **replicated_server.py**: HTTP server with primary-secondary replication
6. **masterless_replication.py**: Master-less replication with vector clocks
7. **masterless_server.py**: HTTP server with master-less replication
8. **indexes.py**: Full-text and embedding indexes

## Installation

No external dependencies required (uses only Python standard library).

```bash
# Clone the repository
git clone <repository-url>
cd NoSQL
```

## Usage

### Single Node Server

```bash
python server.py [port] [debug]
```

Example:
```bash
python server.py 8080 false
```

### Primary-Secondary Replication (3 nodes)

Start 3 nodes:
```bash
# Node 1
python replicated_server.py 1 9001 localhost:9002 localhost:9003

# Node 2
python replicated_server.py 2 9002 localhost:9001 localhost:9003

# Node 3
python replicated_server.py 3 9003 localhost:9001 localhost:9002
```

### Master-less Replication (3 nodes)

Start 3 nodes:
```bash
# Node 1
python masterless_server.py 1 9101 localhost:9102 localhost:9103

# Node 2
python masterless_server.py 2 9102 localhost:9101 localhost:9103

# Node 3
python masterless_server.py 3 9103 localhost:9101 localhost:9102
```

### Using the Client

```python
from client import KVClient

# Connect to server
client = KVClient(host="localhost", port=8080)

# Set a value
client.set("key1", "value1")

# Get a value
value = client.get("key1")
print(value)  # "value1"

# Delete a key
client.delete("key1")

# Bulk set
items = [("key1", "value1"), ("key2", "value2")]
client.bulk_set(items)
```

## Testing

### Run Basic Tests

```bash
python tests.py
```

Tests cover:
- Set then Get
- Set then Delete then Get
- Get without setting
- Set then Set (same key) then Get
- Set then exit gracefully then Get
- Concurrent bulk writes on same keys
- Bulk writes with random server kills

### Run Replication Tests

```bash
python test_replication.py
```

Tests cover:
- Leader election
- Primary failure and re-election
- Data replication

### Run Benchmarks

```bash
python benchmarks.py
```

Benchmarks measure:
- Write throughput with different data sizes
- Durability (data recovery after crashes)

## API Endpoints

### GET /get?key=<key>
Get value for a key.

### POST /set
Set a key-value pair.
```json
{
  "key": "my_key",
  "value": "my_value",
  "simulate_failure": false
}
```

### POST /delete
Delete a key.
```json
{
  "key": "my_key",
  "simulate_failure": false
}
```

### POST /bulk_set
Set multiple key-value pairs.
```json
{
  "items": [
    {"key": "key1", "value": "value1"},
    {"key": "key2", "value": "value2"}
  ],
  "simulate_failure": false
}
```

## Debug Mode

Enable debug mode to simulate file system synchronization issues (1% chance of failure):

```bash
python server.py 8080 true
```

This helps test durability and recovery mechanisms.

## Data Persistence

Data is stored in:
- `data.json`: Main data file
- `wal.log`: Write-Ahead Log for durability
- `data_node_N/`: Per-node data directories for replication

## Replication Details

### Primary-Secondary
- One primary node handles all writes and reads
- Two secondary nodes replicate data from primary
- Leader election using Raft-like algorithm
- Automatic failover when primary fails

### Master-less
- All nodes can handle reads and writes
- Quorum-based consistency (majority required)
- Vector clocks for conflict resolution
- Gossip protocol for state synchronization

## Indexing

### Full-Text Search
```python
from indexes import IndexedKVStore, KeyValueStore

kv_store = KeyValueStore()
indexed_store = IndexedKVStore(kv_store)

indexed_store.set("doc1", "This is a test document")
indexed_store.set("doc2", "Another test document")

# Search
results = indexed_store.fulltext_search("test")
# Returns: ["doc1", "doc2"]
```

### Word Embedding Search
```python
results = indexed_store.embedding_search("test document", top_k=5)
# Returns: [("doc1", 0.95), ("doc2", 0.87), ...]
```

## Performance

- Write throughput: Tested with benchmarks
- Durability: 100% with WAL (Write-Ahead Log)
- Consistency: Strong consistency in primary-secondary mode
- Availability: High availability with replication

## License

MIT License

## Author

Built for ITI NoSQL course project.

