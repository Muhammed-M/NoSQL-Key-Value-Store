"""
Entry point for running a single-node server.
"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

if __name__ == "__main__":
    from server import KVServer
    
    port = 8080
    debug = False
    
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    if len(sys.argv) > 2:
        debug = sys.argv[2].lower() == "true"
    
    server = KVServer(host="localhost", port=port, debug=debug)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

