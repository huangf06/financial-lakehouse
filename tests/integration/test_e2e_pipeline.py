"""End-to-end pipeline evidence placeholder."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_e2e_pipeline_modules_exist() -> None:
    import pipelines.bronze
    import pipelines.gold
    import pipelines.silver

    assert pipelines.bronze and pipelines.silver and pipelines.gold
