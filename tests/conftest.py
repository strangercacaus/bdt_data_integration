import pytest
import pandas as pd
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock
from sqlalchemy import create_engine

# Add the project root to the Python path
project_root = str(Path(__file__).parent.parent.absolute())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

@pytest.fixture
def mock_api_response():
    """Mock API response fixture for testing extractors."""
    return {
        "results": [
            {"id": "1", "name": "Test Item 1", "properties": {"field1": "value1"}},
            {"id": "2", "name": "Test Item 2", "properties": {"field1": "value2"}}
        ],
        "has_more": False,
        "next_cursor": None
    }

@pytest.fixture
def mock_paginated_api_response():
    """Mock paginated API response for testing extractors."""
    return [
        {
            "results": [
                {"id": "1", "name": "Test Item 1", "properties": {"field1": "value1"}},
                {"id": "2", "name": "Test Item 2", "properties": {"field1": "value2"}}
            ],
            "has_more": True,
            "next_cursor": "cursor1"
        },
        {
            "results": [
                {"id": "3", "name": "Test Item 3", "properties": {"field1": "value3"}},
                {"id": "4", "name": "Test Item 4", "properties": {"field1": "value4"}}
            ],
            "has_more": False,
            "next_cursor": None
        }
    ]

@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing loaders."""
    return pd.DataFrame({
        "id": ["1", "2", "3"],
        "name": ["Test Item 1", "Test Item 2", "Test Item 3"],
        "value": [10, 20, 30]
    })

@pytest.fixture
def mock_sqlalchemy_engine():
    """Mock SQLAlchemy engine for testing loaders."""
    engine = MagicMock()
    connection = MagicMock()
    
    # Configure the mock engine to return a mock connection
    engine.connect.return_value = connection
    
    # Create a context manager for the begin method
    cm = MagicMock()
    cm.__enter__.return_value = connection
    engine.begin.return_value = cm
    
    # Make URL accessible for testing
    engine.url = MagicMock()
    engine.url.database = "test_db"
    
    return engine 