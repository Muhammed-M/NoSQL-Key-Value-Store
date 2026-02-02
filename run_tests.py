"""
Entry point for running tests.
"""
import sys
import os

# Add tests directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tests'))

if __name__ == "__main__":
    from tests import *
    
    print("Running tests...\n")
    
    test_set_then_get()
    test_set_then_delete_then_get()
    test_get_without_setting()
    test_set_then_set_same_key_then_get()
    test_set_then_exit_gracefully_then_get()
    test_concurrent_bulk_set_same_keys()
    test_bulk_set_with_random_kill()
    
    print("\nAll tests passed!")

