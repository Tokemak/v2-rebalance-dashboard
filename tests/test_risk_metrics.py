# tests/test_risk_metrics_pages_integration.py

import os
import pytest
from streamlit.testing.v1 import AppTest
from mainnet_launch.constants import CHAIN_BASE_ASSET_GROUPS
from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS_WITH_ARGS

os.environ.setdefault("STREAMLIT_HEADLESS", "1")


def _run_page(chain, base_asset, valid_autopools, _fn_name: str):
    # Import here so the temp Streamlit script has no forward-ref types to resolve
    from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS_WITH_ARGS as _fns

    fn = _fns[_fn_name]
    fn(chain=chain, base_asset=base_asset, valid_autopools=valid_autopools)


def _params():
    for fn_name, _ in RISK_METRICS_FUNCTIONS_WITH_ARGS.items():
        for (chain, base_asset), valid_autopools in CHAIN_BASE_ASSET_GROUPS.items():
            yield pytest.param(
                fn_name,
                {"chain": chain, "base_asset": base_asset, "valid_autopools": valid_autopools},
                id=f"{fn_name}-{chain.name}-{base_asset.name}",
            )


@pytest.mark.parametrize("fn_name,kwargs", list(_params()))
def test_risk_metrics_pages_render_without_error(fn_name, kwargs):
    at = AppTest.from_function(_run_page, kwargs={**kwargs, "_fn_name": fn_name}).run()
    assert at.exception is None, f"Streamlit app raised: {at.exception}"
    assert len(at.delta_generator._root_dg._queue) > 0, "Nothing was rendered to the page"
