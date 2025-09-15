import os
import pytest
from mainnet_launch.constants import ALL_AUTOPOOLS
from mainnet_launch.pages.autopool import AUTOPOOL_CONTENT_FUNCTIONS


os.environ.setdefault("STREAMLIT_HEADLESS", "1")


def _run_page(autopool, _fn_name: str):
    from mainnet_launch.pages.autopool import AUTOPOOL_CONTENT_FUNCTIONS

    fn = AUTOPOOL_CONTENT_FUNCTIONS[_fn_name]
    fn(autopool=autopool)


def _param_generator():
    for fn_name, _ in AUTOPOOL_CONTENT_FUNCTIONS.items():
        for autopool in ALL_AUTOPOOLS:
            yield pytest.param(
                fn_name,
                {"autopool": autopool},
                id=f"{fn_name}-{autopool.name}",
            )


@pytest.mark.parametrize("fn_name,kwargs", list(_param_generator()))
def test_autopool_pages(fn_name, kwargs):
    _run_page(**kwargs, _fn_name=fn_name)
