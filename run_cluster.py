"""
Helper script to run a cluster of nodes.
"""
import subprocess
import sys
import time


def run_replicated_cluster():
    """Run a 3-node primary-secondary cluster."""
    print("Starting 3-node primary-secondary cluster...")
    
    processes = []
    try:
        # Start node 1
        p1 = subprocess.Popen(
            ["python", "replicated_server.py", "1", "9001", "localhost:9002", "localhost:9003"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(p1)
        print("Node 1 started on port 9001")
        time.sleep(0.5)
        
        # Start node 2
        p2 = subprocess.Popen(
            ["python", "replicated_server.py", "2", "9002", "localhost:9001", "localhost:9003"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(p2)
        print("Node 2 started on port 9002")
        time.sleep(0.5)
        
        # Start node 3
        p3 = subprocess.Popen(
            ["python", "replicated_server.py", "3", "9003", "localhost:9001", "localhost:9002"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(p3)
        print("Node 3 started on port 9003")
        time.sleep(2)
        
        print("\nCluster is running. Press Ctrl+C to stop.")
        print("Connect to any node using:")
        print("  from client import KVClient")
        print("  client = KVClient(port=9001)  # or 9002, 9003")
        
        # Wait for interrupt
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping cluster...")
        for p in processes:
            p.terminate()
            p.wait()
        print("Cluster stopped.")


def run_masterless_cluster():
    """Run a 3-node master-less cluster."""
    print("Starting 3-node master-less cluster...")
    
    processes = []
    try:
        # Start node 1
        p1 = subprocess.Popen(
            ["python", "masterless_server.py", "1", "9101", "localhost:9102", "localhost:9103"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(p1)
        print("Node 1 started on port 9101")
        time.sleep(0.5)
        
        # Start node 2
        p2 = subprocess.Popen(
            ["python", "masterless_server.py", "2", "9102", "localhost:9101", "localhost:9103"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(p2)
        print("Node 2 started on port 9102")
        time.sleep(0.5)
        
        # Start node 3
        p3 = subprocess.Popen(
            ["python", "masterless_server.py", "3", "9103", "localhost:9101", "localhost:9102"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(p3)
        print("Node 3 started on port 9103")
        time.sleep(2)
        
        print("\nCluster is running. Press Ctrl+C to stop.")
        print("Connect to any node using:")
        print("  from client import KVClient")
        print("  client = KVClient(port=9101)  # or 9102, 9103")
        
        # Wait for interrupt
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping cluster...")
        for p in processes:
            p.terminate()
            p.wait()
        print("Cluster stopped.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "masterless":
        run_masterless_cluster()
    else:
        run_replicated_cluster()

