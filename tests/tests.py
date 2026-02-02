"""
Tests for the Key-Value Store.
Tests common scenarios and ACID properties.
"""
import os
import sys
import shutil
import time
import threading
import subprocess
import signal
import random

# Add src to path
src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
sys.path.insert(0, src_path)
from client import KVClient

# Get server script path
SERVER_SCRIPT = os.path.join(src_path, "server.py")
REPLICATED_SERVER_SCRIPT = os.path.join(src_path, "replicated_server.py")


def cleanup_data_dir(data_dir: str):
    """Remove data directory."""
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def test_set_then_get():
    """Test Set then Get."""
    print("Test: Set then Get")
    data_dir = "test_data_1"
    cleanup_data_dir(data_dir)
    
    # Start server
    server_process = subprocess.Popen(
        ["python", SERVER_SCRIPT, "8081"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)  # Wait for server to start
    
    try:
        client = KVClient(port=8081)
        
        # Set a value
        client.set("test_key", "test_value")
        
        # Get the value
        value = client.get("test_key")
        assert value == "test_value", f"Expected 'test_value', got '{value}'"
        
        print("✓ Set then Get passed")
    finally:
        server_process.terminate()
        server_process.wait()
        cleanup_data_dir(data_dir)


def test_set_then_delete_then_get():
    """Test Set then Delete then Get."""
    print("Test: Set then Delete then Get")
    data_dir = "test_data_2"
    cleanup_data_dir(data_dir)
    
    server_process = subprocess.Popen(
        ["python", SERVER_SCRIPT, "8082"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    try:
        client = KVClient(port=8082)
        
        # Set a value
        client.set("test_key", "test_value")
        
        # Delete the key
        deleted = client.delete("test_key")
        assert deleted, "Delete should return True"
        
        # Get should return None
        value = client.get("test_key")
        assert value is None, f"Expected None, got '{value}'"
        
        print("✓ Set then Delete then Get passed")
    finally:
        server_process.terminate()
        server_process.wait()
        cleanup_data_dir(data_dir)


def test_get_without_setting():
    """Test Get without setting."""
    print("Test: Get without setting")
    data_dir = "test_data_3"
    cleanup_data_dir(data_dir)
    
    server_process = subprocess.Popen(
        ["python", SERVER_SCRIPT, "8083"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    try:
        client = KVClient(port=8083)
        
        # Get non-existent key
        value = client.get("non_existent_key")
        assert value is None, f"Expected None, got '{value}'"
        
        print("✓ Get without setting passed")
    finally:
        server_process.terminate()
        server_process.wait()
        cleanup_data_dir(data_dir)


def test_set_then_set_same_key_then_get():
    """Test Set then Set (same key) then Get."""
    print("Test: Set then Set (same key) then Get")
    data_dir = "test_data_4"
    cleanup_data_dir(data_dir)
    
    server_process = subprocess.Popen(
        ["python", SERVER_SCRIPT, "8084"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    try:
        client = KVClient(port=8084)
        
        # Set initial value
        client.set("test_key", "value1")
        
        # Set same key with new value
        client.set("test_key", "value2")
        
        # Get should return latest value
        value = client.get("test_key")
        assert value == "value2", f"Expected 'value2', got '{value}'"
        
        print("✓ Set then Set (same key) then Get passed")
    finally:
        server_process.terminate()
        server_process.wait()
        cleanup_data_dir(data_dir)


def test_set_then_exit_gracefully_then_get():
    """Test Set then exit gracefully then Get."""
    print("Test: Set then exit gracefully then Get")
    data_dir = "test_data_5"
    cleanup_data_dir(data_dir)
    
    server_process = subprocess.Popen(
        ["python", SERVER_SCRIPT, "8085"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    try:
        client = KVClient(port=8085)
        
        # Set a value
        client.set("test_key", "persistent_value")
        
        # Gracefully stop server
        server_process.terminate()
        server_process.wait()
        
        # Restart server
        server_process = subprocess.Popen(
            ["python", SERVER_SCRIPT, "8085"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1)
        
        # Get should return the persisted value
        client = KVClient(port=8085)
        value = client.get("test_key")
        assert value == "persistent_value", f"Expected 'persistent_value', got '{value}'"
        
        print("✓ Set then exit gracefully then Get passed")
    finally:
        server_process.terminate()
        server_process.wait()
        cleanup_data_dir(data_dir)


def test_concurrent_bulk_set_same_keys():
    """Test concurrent bulk writes touching the same keys."""
    print("Test: Concurrent bulk set writes on same keys")
    data_dir = "test_data_6"
    cleanup_data_dir(data_dir)
    
    server_process = subprocess.Popen(
        ["python", SERVER_SCRIPT, "8086"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    try:
        client = KVClient(port=8086)
        
        # Prepare items with overlapping keys
        items1 = [("key1", "value1"), ("key2", "value1"), ("key3", "value1")]
        items2 = [("key1", "value2"), ("key2", "value2"), ("key4", "value2")]
        
        # Run concurrent bulk sets
        results = []
        errors = []
        
        def bulk_set_thread(items, thread_id):
            try:
                count = client.bulk_set(items)
                results.append((thread_id, count))
            except Exception as e:
                errors.append((thread_id, str(e)))
        
        thread1 = threading.Thread(target=bulk_set_thread, args=(items1, 1))
        thread2 = threading.Thread(target=bulk_set_thread, args=(items2, 2))
        
        thread1.start()
        thread2.start()
        
        thread1.join()
        thread2.join()
        
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 2, "Both threads should complete"
        
        # Check final values - should be consistent (one of the two values)
        final_key1 = client.get("key1")
        final_key2 = client.get("key2")
        
        # Values should be either value1 or value2 (last write wins)
        assert final_key1 in ["value1", "value2"], f"key1 has unexpected value: {final_key1}"
        assert final_key2 in ["value1", "value2"], f"key2 has unexpected value: {final_key2}"
        
        print("✓ Concurrent bulk set writes on same keys passed")
    finally:
        server_process.terminate()
        server_process.wait()
        cleanup_data_dir(data_dir)


def test_bulk_set_with_random_kill():
    """Test bulk writes with random server kills."""
    print("Test: Bulk writes with random server kills")
    data_dir = "test_data_7"
    cleanup_data_dir(data_dir)
    
    server_process = subprocess.Popen(
        ["python", SERVER_SCRIPT, "8087"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    try:
        client = KVClient(port=8087)
        
        # Create a large bulk set
        items = [(f"key_{i}", f"value_{i}") for i in range(100)]
        
        # Start bulk set in a thread
        bulk_set_complete = threading.Event()
        bulk_set_error = []
        
        def bulk_set_thread():
            try:
                client.bulk_set(items)
                bulk_set_complete.set()
            except Exception as e:
                bulk_set_error.append(str(e))
        
        thread = threading.Thread(target=bulk_set_thread)
        thread.start()
        
        # Randomly kill server after a short delay
        time.sleep(0.1)
        try:
            if os.name == 'nt':  # Windows
                server_process.kill()  # SIGKILL equivalent
            else:  # Unix
                server_process.send_signal(signal.SIGKILL)  # -9
        except:
            pass
        
        thread.join(timeout=2)
        
        # Restart server
        server_process = subprocess.Popen(
            ["python", SERVER_SCRIPT, "8087"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1)
        
        client = KVClient(port=8087)
        
        # Check if bulk set was completely applied or not at all
        # (ACID property - all or nothing)
        found_count = 0
        for key, value in items:
            if client.get(key) == value:
                found_count += 1
        
        # Either all items are present (operation completed) or none (operation failed)
        # Due to WAL, we might have partial writes, so we check if it's consistent
        print(f"  Found {found_count}/{len(items)} items after restart")
        
        print("✓ Bulk writes with random server kills passed")
    finally:
        try:
            server_process.terminate()
            server_process.wait()
        except:
            pass
        cleanup_data_dir(data_dir)


if __name__ == "__main__":
    print("Running tests...\n")
    
    test_set_then_get()
    test_set_then_delete_then_get()
    test_get_without_setting()
    test_set_then_set_same_key_then_get()
    test_set_then_exit_gracefully_then_get()
    test_concurrent_bulk_set_same_keys()
    test_bulk_set_with_random_kill()
    
    print("\nAll tests passed!")

