# Contributing Guide

Contributions to roastcoffea are welcome! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.12 or later
- [pixi](https://pixi.sh/) package manager

### Installing for Development

Clone the repository and install in development mode:

```bash
git clone https://github.com/MoAly98/roastcoffea.git
cd roastcoffea
pixi install
```

This sets up the development environment with all dependencies, including testing and documentation tools.

## Running Tests

Run the test suite:

```bash
pixi run -e dev pytest
```

Run with coverage:

```bash
pixi run -e dev pytest --cov=src/roastcoffea --cov-report=term
```

Run specific test markers:

```bash
pixi run -e dev pytest -m "not slow"  # Skip slow tests
pixi run -e dev pytest -m slow        # Only slow tests
```

## Code Quality

### Pre-commit Hooks

Install and run pre-commit hooks:

```bash
pixi run -e dev pre-commit install
pixi run -e dev pre-commit run --all-files
```

The hooks check for:
- Code formatting (ruff format)
- Linting (ruff)
- Type checking (mypy)
- Common issues (trailing whitespace, etc.)

### Code Style

- Follow PEP 8 conventions
- Use type hints for all function signatures
- Write docstrings in Google style
- Keep functions focused and modular
- Prefer explicit over implicit

## Documentation

### Building Documentation

Build the documentation locally:

```bash
pixi run -e dev sphinx-build -b html docs docs/_build/html
```

View the built docs by opening `docs/_build/html/index.html` in your browser.

### Documentation Style

- Use MyST Markdown format
- Include code examples that users can copy-paste
- Add cross-references to related sections
- Keep language clear and concise
- Include both "what" and "why" explanations

## Adding New Features

### Workflow

1. **Create an issue** describing the feature or bug
2. **Fork and branch** from `main`
3. **Implement** with tests
4. **Update documentation** if needed
5. **Run tests and pre-commit** to verify
6. **Submit a pull request**

### Adding a New Backend

To support a new executor (e.g., Spark, Ray):

1. Implement `AbstractMetricsBackend` in `src/roastcoffea/backends/`
2. Add the backend to `get_parser()` in `src/roastcoffea/aggregation/backends.py`
3. Write tests in `tests/backends/`
4. Update documentation in `docs/advanced.md`

See {doc}`advanced` for implementation details.

### Adding New Metrics

1. Collect raw data in the appropriate backend or decorator
2. Add aggregation logic in `src/roastcoffea/aggregation/`
3. Update the reporter in `src/roastcoffea/export/reporter.py`
4. Document in `docs/metrics_reference.md`
5. Write tests covering the new metric

## Testing Guidelines

### Test Structure

- **Unit tests** in `tests/` - Test individual functions and classes
- **Integration tests** - Test component interactions
- **End-to-end tests** in `tests/test_e2e.py` - Test complete workflows

### Writing Good Tests

- Test both success and failure cases
- Use descriptive test names: `test_metric_aggregation_with_missing_data`
- Isolate tests - no dependencies between tests
- Use fixtures for common setup
- Mock external dependencies when appropriate

### Test Coverage

Aim for high coverage but focus on meaningful tests:
- All public APIs should be tested
- Edge cases and error paths
- Integration between components

## Submitting Pull Requests

### PR Checklist

- [ ] Tests pass locally
- [ ] Pre-commit hooks pass
- [ ] Documentation updated if needed
- [ ] CHANGELOG updated (if applicable)
- [ ] Descriptive commit messages
- [ ] PR description explains changes

### Commit Messages

Use semantic commit format:

```
feat: add Spark backend support
fix: correct overhead calculation for retried tasks
docs: update quickstart with new API
test: add integration tests for chunk metrics
```

### Review Process

1. Maintainers will review your PR
2. Address feedback and push updates
3. Once approved, maintainers will merge

## Project Structure

```
roastcoffea/
├── src/roastcoffea/
│   ├── backends/          # Executor-specific backends
│   ├── aggregation/       # Metrics aggregation logic
│   ├── export/            # Output formatting and export
│   ├── visualization/     # Plotting utilities
│   ├── collector.py       # Main MetricsCollector class
│   └── decorator.py       # @track_metrics decorator
├── tests/                 # Test suite
├── docs/                  # Documentation source
└── examples/              # Example notebooks
```

## Getting Help

- **Issues**: Report bugs or request features on [GitHub Issues](https://github.com/MoAly98/roastcoffea/issues)
- **Discussions**: Ask questions on [GitHub Discussions](https://github.com/MoAly98/roastcoffea/discussions)
- **Documentation**: Check the full docs at [roastcoffea.readthedocs.io](https://roastcoffea.readthedocs.io)

## Code of Conduct

Be respectful, inclusive, and constructive. We're all here to advance HEP computing together.

## License

By contributing, you agree that your contributions will be licensed under the BSD-3-Clause License.
