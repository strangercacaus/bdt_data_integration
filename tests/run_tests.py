#!/usr/bin/env python
"""
Test runner script for bdt_data_integration.
This script runs all the tests using pytest.
"""

import pytest
import sys
import os
from pathlib import Path

if __name__ == "__main__":
    # Set up the Python path properly
    project_root = Path(__file__).parent.parent.absolute()
    
    # Use a more direct method to run the tests with the right Python path
    sys.path.insert(0, str(project_root))
    
    # Run using the pytest module directly with proper arguments
    exit_code = pytest.main([
        "--verbose",
        "--color=yes",
        "-p", "no:warnings",  # Suppress warnings
        str(Path(__file__).parent)
    ])
    
    sys.exit(exit_code) 