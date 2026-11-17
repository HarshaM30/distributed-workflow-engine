"""Apply the unit/integration/chaos marker from each test's directory."""

from __future__ import annotations

from pathlib import Path

import pytest

_SUITES = ("unit", "integration", "chaos")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        parts = Path(str(item.path)).parts
        for suite in _SUITES:
            if suite in parts:
                item.add_marker(getattr(pytest.mark, suite))
