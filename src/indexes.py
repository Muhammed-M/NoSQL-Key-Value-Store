"""
Indexing system for the Key-Value Store.
Supports full-text search and word embedding indexes.
"""
import json
import os
import re
from typing import Dict, List, Set, Optional
from collections import defaultdict


class FullTextIndex:
    """Full-text search index using inverted index."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize full-text index.
        
        Args:
            data_dir: Data directory
        """
        self.data_dir = data_dir
        self.index_file = os.path.join(data_dir, "fulltext_index.json")
        self.inverted_index: Dict[str, Set[str]] = defaultdict(set)  # word -> set of keys
        self._load_index()
    
    def _load_index(self):
        """Load index from disk."""
        if os.path.exists(self.index_file):
            try:
                with open(self.index_file, "r") as f:
                    data = json.load(f)
                    # Convert lists back to sets
                    self.inverted_index = {word: set(keys) for word, keys in data.items()}
            except (json.JSONDecodeError, IOError):
                self.inverted_index = defaultdict(set)
    
    def _save_index(self):
        """Save index to disk."""
        os.makedirs(self.data_dir, exist_ok=True)
        # Convert sets to lists for JSON serialization
        data = {word: list(keys) for word, keys in self.inverted_index.items()}
        with open(self.index_file, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
    
    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into words."""
        # Simple tokenization: split on whitespace and punctuation
        words = re.findall(r'\b\w+\b', text.lower())
        return words
    
    def index_value(self, key: str, value: str):
        """Index a value for a key."""
        # Remove old index entries for this key
        self.remove_key(key)
        
        # Tokenize and index
        words = self._tokenize(value)
        for word in words:
            self.inverted_index[word].add(key)
        
        self._save_index()
    
    def remove_key(self, key: str):
        """Remove all index entries for a key."""
        for word in list(self.inverted_index.keys()):
            self.inverted_index[word].discard(key)
            if not self.inverted_index[word]:
                del self.inverted_index[word]
        self._save_index()
    
    def search(self, query: str) -> List[str]:
        """
        Search for keys matching the query.
        
        Args:
            query: Search query
        
        Returns:
            List of keys matching the query
        """
        query_words = self._tokenize(query)
        if not query_words:
            return []
        
        # Find intersection of all query words (AND search)
        result_keys = None
        for word in query_words:
            if word in self.inverted_index:
                if result_keys is None:
                    result_keys = self.inverted_index[word].copy()
                else:
                    result_keys &= self.inverted_index[word]
            else:
                # Word not found, return empty result
                return []
        
        return list(result_keys) if result_keys else []


class WordEmbeddingIndex:
    """Word embedding index for semantic search."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize word embedding index.
        
        Args:
            data_dir: Data directory
        """
        self.data_dir = data_dir
        self.index_file = os.path.join(data_dir, "embedding_index.json")
        self.embeddings: Dict[str, List[float]] = {}  # key -> embedding vector
        self._load_index()
    
    def _load_index(self):
        """Load embeddings from disk."""
        if os.path.exists(self.index_file):
            try:
                with open(self.index_file, "r") as f:
                    self.embeddings = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.embeddings = {}
    
    def _save_index(self):
        """Save embeddings to disk."""
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.index_file, "w") as f:
            json.dump(self.embeddings, f)
            f.flush()
            os.fsync(f.fileno())
    
    def _simple_embedding(self, text: str) -> List[float]:
        """
        Simple embedding using character frequency and length.
        In production, use proper word embeddings like Word2Vec, BERT, etc.
        
        Args:
            text: Text to embed
        
        Returns:
            Embedding vector
        """
        # Simple embedding: character frequency vector + length
        text_lower = text.lower()
        # Character frequency (a-z)
        char_freq = [text_lower.count(chr(ord('a') + i)) for i in range(26)]
        # Normalize by text length
        text_len = len(text) if text else 1
        char_freq_normalized = [f / text_len for f in char_freq]
        # Add length feature
        char_freq_normalized.append(len(text) / 100.0)  # Normalize length
        return char_freq_normalized
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        if len(vec1) != len(vec2):
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = sum(a * a for a in vec1) ** 0.5
        magnitude2 = sum(b * b for b in vec2) ** 0.5
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def index_value(self, key: str, value: str):
        """Index a value with its embedding."""
        embedding = self._simple_embedding(value)
        self.embeddings[key] = embedding
        self._save_index()
    
    def remove_key(self, key: str):
        """Remove embedding for a key."""
        self.embeddings.pop(key, None)
        self._save_index()
    
    def search(self, query: str, top_k: int = 10) -> List[tuple]:
        """
        Search for similar values using embedding similarity.
        
        Args:
            query: Search query
            top_k: Number of top results to return
        
        Returns:
            List of (key, similarity_score) tuples, sorted by similarity
        """
        query_embedding = self._simple_embedding(query)
        
        similarities = []
        for key, embedding in self.embeddings.items():
            similarity = self._cosine_similarity(query_embedding, embedding)
            similarities.append((key, similarity))
        
        # Sort by similarity (descending)
        similarities.sort(key=lambda x: x[1], reverse=True)
        
        return similarities[:top_k]


class IndexedKVStore:
    """Key-Value Store with indexing support."""
    
    def __init__(self, kv_store, data_dir: str = "data"):
        """
        Initialize indexed KV store.
        
        Args:
            kv_store: Underlying KV store instance
            data_dir: Data directory
        """
        self.kv_store = kv_store
        self.fulltext_index = FullTextIndex(data_dir=data_dir)
        self.embedding_index = WordEmbeddingIndex(data_dir=data_dir)
    
    def set(self, key: str, value: str, simulate_failure: bool = False) -> bool:
        """Set a key-value pair and update indexes."""
        success = self.kv_store.set(key, value, simulate_failure=simulate_failure)
        if success:
            self.fulltext_index.index_value(key, value)
            self.embedding_index.index_value(key, value)
        return success
    
    def get(self, key: str) -> Optional[str]:
        """Get value for a key."""
        return self.kv_store.get(key)
    
    def delete(self, key: str, simulate_failure: bool = False) -> bool:
        """Delete a key and remove from indexes."""
        success = self.kv_store.delete(key, simulate_failure=simulate_failure)
        if success:
            self.fulltext_index.remove_key(key)
            self.embedding_index.remove_key(key)
        return success
    
    def bulk_set(self, items: List[tuple], simulate_failure: bool = False) -> int:
        """Set multiple key-value pairs and update indexes."""
        count = self.kv_store.bulk_set(items, simulate_failure=simulate_failure)
        # Update indexes
        for key, value in items:
            self.fulltext_index.index_value(key, value)
            self.embedding_index.index_value(key, value)
        return count
    
    def fulltext_search(self, query: str) -> List[str]:
        """Search using full-text index."""
        return self.fulltext_index.search(query)
    
    def embedding_search(self, query: str, top_k: int = 10) -> List[tuple]:
        """Search using embedding similarity."""
        return self.embedding_index.search(query, top_k=top_k)

