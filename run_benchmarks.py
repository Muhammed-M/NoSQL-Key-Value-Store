"""
Entry point for running benchmarks.
"""
import sys
import os

# Add benchmarks directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'benchmarks'))

if __name__ == "__main__":
    from benchmarks import *
    
    benchmark_write_throughput()
    benchmark_durability()
    
    print("\nBenchmarks completed!")

