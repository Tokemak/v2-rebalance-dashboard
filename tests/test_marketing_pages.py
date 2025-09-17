"""Tests that none of the marketing pages throw errors when run. Does not check that the content is correct."""

import os
import pytest

from mainnet_launch.app.marketing_app.marketing_pages import (
    MARKETING_PAGES_WITH_NO_ARGS,
    MARKETING_PAGES_WITH_AUTOPOOL_ARG,
)
from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants


os.environ.setdefault("STREAMLIT_HEADLESS", "1")


def _run_pages_with_no_args(_fn_name: str):
    from mainnet_launch.app.marketing_app.marketing_pages import MARKETING_PAGES_WITH_NO_ARGS as _fns

    fn = _fns[_fn_name]
    fn()


def _marketing_app_no_arg_pages():
    for fn_name, _ in MARKETING_PAGES_WITH_NO_ARGS.items():
        yield pytest.param(
            fn_name,
            {},
            id=f"protocol-{fn_name}",
        )


@pytest.mark.marketing
@pytest.mark.parametrize("fn_name,kwargs", list(_marketing_app_no_arg_pages()))
def test_marketing_pages_no_args(fn_name, kwargs):
    _run_pages_with_no_args(**kwargs, _fn_name=fn_name)


def _run_marketing_autopool_page(autopool: AutopoolConstants, _fn_name: str):
    from mainnet_launch.app.marketing_app.marketing_pages import MARKETING_PAGES_WITH_AUTOPOOL_ARG as _fns

    fn = _fns[_fn_name]
    fn(autopool=autopool)


def _marketing_autopool_params():
    for fn_name, _ in MARKETING_PAGES_WITH_AUTOPOOL_ARG.items():
        for autopool in ALL_AUTOPOOLS:
            yield pytest.param(
                fn_name,
                {"autopool": autopool},
                id=f"autopool-{fn_name}-{autopool.name}",
            )


@pytest.mark.marketing
@pytest.mark.parametrize("fn_name,kwargs", list(_marketing_autopool_params()))
def test_marketing_autopool_pages(fn_name, kwargs):
    _run_marketing_autopool_page(**kwargs, _fn_name=fn_name)
