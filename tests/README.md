# Tests for BDT Data Integration

This directory contains tests for the extractors and loaders classes of the BDT Data Integration project.

## Test Structure

- `extractors/`: Tests for extractor classes
  - `test_base_extractor.py`: Tests for the base extractor classes
  - `test_notion_extractor.py`: Tests for the Notion API extractor
- `loaders/`: Tests for loader classes
  - `test_base_loader.py`: Tests for the base loader class
  - `test_postgres_loader.py`: Tests for the PostgreSQL loader
- `conftest.py`: Common fixtures used across tests
- `run_tests.py`: Script to run all tests (Note: Currently has import path issues)

## Running Tests

### Running Specific Tests

The most reliable way to run tests is to run them individually:

```bash
# Run extractor tests
python -m pytest tests/extractors/test_base_extractor.py -v
python -m pytest tests/extractors/test_notion_extractor.py -v

# Run loader tests
python -m pytest tests/loaders/test_base_loader.py -v
python -m pytest tests/loaders/test_postgres_loader.py -v
```

You can also run all tests for a specific module:

```bash
# Run only extractor tests
python -m pytest tests/extractors/ -v

# Run only loader tests
python -m pytest tests/loaders/ -v
```

## Test Coverage

The current tests cover:

1. **Base Classes**:
   - `GenericExtractor`, `GenericAPIExtractor`, and `GenericDatabaseExtractor`
   - `BaseLoader`

2. **Concrete Implementations**:
   - `NotionDatabaseAPIExtractor`
   - `PostgresLoader`

## Writing New Tests

When adding new extractor or loader classes, please add corresponding test files following the existing patterns:

1. Create a new test file in the appropriate directory (`extractors/` or `loaders/`)
2. Use pytest fixtures for test setup
3. Use the pytest-mock plugin for mocking external dependencies
4. Follow the existing test naming conventions
5. Aim for high test coverage of all class methods

## Mocking Strategies

- Use the `patch` decorator to mock external dependencies
- Use `MagicMock` for more complex mocking scenarios
- For methods with context managers, use the `patch.object` approach
- For difficult-to-test code, consider simplifying the test by patching the method itself 