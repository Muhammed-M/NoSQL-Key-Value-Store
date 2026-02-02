"""
Benchmarks for the Key-Value Store.
Tests write throughput and durability.
"""
import os
import shutil
import time
import threading
import subprocess
import signal
import random
from client import KVClient


def cleanup_data_dir(data_dir: str):
    """Remove data directory."""
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def benchmark_write_throughput():
    """Benchmark write throughput with pre-populated data."""
    print("Benchmark: Write Throughput")
    print("=" * 50)
    
    data_dirs = ["bench_data_1", "bench_data_2", "bench_data_3"]
    data_sizes = [0, 1000, 10000]  # Pre-populate with different amounts
    
    for data_dir, pre_populate_size in zip(data_dirs, data_sizes):
        cleanup_data_dir(data_dir)
        
        server_process = subprocess.Popen(
            ["python", "server.py", "8090"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1)
        
        try:
            client = KVClient(port=8090)
            
            # Pre-populate data
            if pre_populate_size > 0:
                print(f"Pre-populating with {pre_populate_size} items...")
                pre_items = [(f"pre_key_{i}", f"pre_value_{i}") for i in range(pre_populate_size)]
                client.bulk_set(pre_items)
            
            # Benchmark writes
            num_writes = 1000
            write_items = [(f"write_key_{i}", f"write_value_{i}") for i in range(num_writes)]
            
            start_time = time.time()
            for key, value in write_items:
                client.set(key, value)
            end_time = time.time()
            
            elapsed = end_time - start_time
            throughput = num_writes / elapsed
            
            print(f"Pre-populated size: {pre_populate_size}")
            print(f"Writes: {num_writes}")
            print(f"Time: {elapsed:.2f}s")
            print(f"Throughput: {throughput:.2f} writes/sec")
            print("-" * 50)
        finally:
            server_process.terminate()
            server_process.wait()
            cleanup_data_dir(data_dir)


def benchmark_durability():
    """Benchmark durability by killing server randomly during writes."""
    print("\nBenchmark: Durability")
    print("=" * 50)
    
    data_dir = "bench_data_durability"
    cleanup_data_dir(data_dir)
    
    server_process = subprocess.Popen(
        ["python", "server.py", "8091"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    acknowledged_keys = []
    write_lock = threading.Lock()
    stop_writing = threading.Event()
    
    def write_thread():
        """Thread that continuously writes data."""
        client = KVClient(port=8091)
        key_counter = 0
        
        while not stop_writing.is_set():
            key = f"durability_key_{key_counter}"
            value = f"durability_value_{key_counter}"
            
            try:
                success = client.set(key, value)
                if success:
                    with write_lock:
                        acknowledged_keys.append((key, value))
                key_counter += 1
                time.sleep(0.001)  # Small delay
            except:
                # Server might be killed
                break
    
    def kill_thread():
        """Thread that kills the server randomly."""
        time.sleep(random.uniform(0.5, 2.0))  # Random delay
        stop_writing.set()
        try:
            if os.name == 'nt':  # Windows
                server_process.kill()  # SIGKILL equivalent
            else:  # Unix
                server_process.send_signal(signal.SIGKILL)  # -9
        except:
            pass
    
    # Start writing thread
    write_thread_obj = threading.Thread(target=write_thread)
    write_thread_obj.start()
    
    # Start kill thread
    kill_thread_obj = threading.Thread(target=kill_thread)
    kill_thread_obj.start()
    
    # Wait for threads
    write_thread_obj.join(timeout=5)
    kill_thread_obj.join()
    
    # Restart server
    time.sleep(0.5)
    server_process = subprocess.Popen(
        ["python", "server.py", "8091"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    # Check which acknowledged keys are still present
    client = KVClient(port=8091)
    recovered_keys = []
    lost_keys = []
    
    with write_lock:
        for key, expected_value in acknowledged_keys:
            actual_value = client.get(key)
            if actual_value == expected_value:
                recovered_keys.append(key)
            else:
                lost_keys.append(key)
    
    total_acknowledged = len(acknowledged_keys)
    total_recovered = len(recovered_keys)
    total_lost = len(lost_keys)
    durability_percentage = (total_recovered / total_acknowledged * 100) if total_acknowledged > 0 else 0
    
    print(f"Total acknowledged writes: {total_acknowledged}")
    print(f"Recovered after restart: {total_recovered}")
    print(f"Lost after restart: {total_lost}")
    print(f"Durability: {durability_percentage:.2f}%")
    print("-" * 50)
    
    server_process.terminate()
    server_process.wait()
    cleanup_data_dir(data_dir)
    
    return durability_percentage


if __name__ == "__main__":
    benchmark_write_throughput()
    benchmark_durability()
    
    print("\nBenchmarks completed!")

