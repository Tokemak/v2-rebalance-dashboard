# tests/test_risk_metrics_pages_integration.py
from __future__ import annotations

import os
import pytest
from streamlit.testing.v1 import AppTest

from mainnet_launch.constants import CHAIN_BASE_ASSET_GROUPS
from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS_WITH_ARGS
import traceback


# Optional: make Streamlit behave fully headless in CI
os.environ.setdefault("STREAMLIT_HEADLESS", "1")


def _params():
    """Yield pytest.param objects with readable IDs for each combo."""
    for fn_name, page_fn in RISK_METRICS_FUNCTIONS_WITH_ARGS.items():
        for (chain, base_asset), valid_autopools in CHAIN_BASE_ASSET_GROUPS.items():
            test_id = f"{fn_name}-{chain.name}-{base_asset.name}"
            yield pytest.param(
                page_fn,
                {"chain": chain, "base_asset": base_asset, "valid_autopools": valid_autopools},
                id=test_id,
            )


@pytest.mark.parametrize("page_fn,kwargs", list(_params()))
def test_risk_metrics_pages_render_without_error(page_fn, kwargs):
    """
    End-to-end smoke test: calling each risk-metrics page function with its
    (chain, base_asset, valid_autopools) args should not raise and should render something.
    """
    try:
        # Run the Streamlit app function with kwargs
        at = AppTest.from_function(page_fn, kwargs=kwargs).run()

        # Assert the app didn't crash
        assert at.exception is None, f"Streamlit app raised: {at.exception}"

        # Minimal sanity: expect at least one element rendered (markdown/text/plot/etc.)
        # Accessing a broad container is safest across versions.
        assert len(at.delta_generator._root_dg._queue) > 0, "Nothing was rendered to the page"
    except Exception as e:
        # Print the entire stack trace for debugging
        traceback.print_exc()
        raise AssertionError(f"Test failed with error: {e}") from e
