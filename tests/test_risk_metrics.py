# tests/test_risk_metrics_pages_integration.py

# note this doesn't use Streamlit's AppTest, TODO switch over to using AppTest for robustnes
# this just checks that the funcitons run without error
# it does not check outputs or buttons

import os
import pytest
from streamlit.testing.v1 import AppTest
from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS_WITH_ARGS
from mainnet_launch.constants import CHAIN_BASE_ASSET_GROUPS

os.environ.setdefault("STREAMLIT_HEADLESS", "1")


def _run_page(chain, base_asset, valid_autopools, _fn_name: str):
    # Import here so the temp Streamlit script has no forward-ref types to resolve
    from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS_WITH_ARGS as _fns

    fn = _fns[_fn_name]
    fn(chain=chain, base_asset=base_asset, valid_autopools=valid_autopools)


def _param_generator():
    for fn_name, _ in RISK_METRICS_FUNCTIONS_WITH_ARGS.items():
        for (chain, base_asset), valid_autopools in CHAIN_BASE_ASSET_GROUPS.items():
            yield pytest.param(
                fn_name,
                {"chain": chain, "base_asset": base_asset, "valid_autopools": valid_autopools},
                id=f"{fn_name}-{chain.name}-{base_asset.name}",
            )


@pytest.mark.parametrize("fn_name,kwargs", list(_param_generator()))
def test_risk_metrics_pages(fn_name, kwargs):
    """Smoke test: ensure each risk metrics page function runs without error."""
    _run_page(**kwargs, _fn_name=fn_name)
