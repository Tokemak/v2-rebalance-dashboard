"""Tests that none of the pages throw errors when run. Does not check that the content is correct."""

import os
import datetime
import pandas as pd
import pytest
import streamlit as st

from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    CURRENT_AUTOPOOLS,
    AUTO_ETH,
    AUTO_USD,
    AutopoolConstants,
    CHAIN_BASE_ASSET_GROUPS,
    SessionState,
)
from mainnet_launch.pages.protocol_wide import PROTOCOL_CONTENT_FUNCTIONS
from mainnet_launch.pages.autopool import AUTOPOOL_CONTENT_FUNCTIONS
from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS_WITH_ARGS


def _run_protocol_page(_fn_name: str):
    from mainnet_launch.pages.protocol_wide import PROTOCOL_CONTENT_FUNCTIONS as _fns

    fn = _fns[_fn_name]
    fn()


def _protocol_params():
    for fn_name, _ in PROTOCOL_CONTENT_FUNCTIONS.items():
        yield pytest.param(
            fn_name,
            {},
            id=f"protocol-{fn_name}",
        )


@pytest.mark.parametrize("fn_name,kwargs", list(_protocol_params()))
def test_protocol_wide_pages(fn_name, kwargs):
    _run_protocol_page(**kwargs, _fn_name=fn_name)


def _run_autopool_page(autopool: AutopoolConstants, _fn_name: str):
    from mainnet_launch.pages.autopool import AUTOPOOL_CONTENT_FUNCTIONS as _fns

    fn = _fns[_fn_name]
    fn(autopool=autopool)


def _autopool_params():
    for fn_name, _ in AUTOPOOL_CONTENT_FUNCTIONS.items():
        for autopool in CURRENT_AUTOPOOLS:
            yield pytest.param(
                fn_name,
                {"autopool": autopool},
                id=f"autopool-{fn_name}-{autopool.name}",
            )


@pytest.mark.parametrize("fn_name,kwargs", list(_autopool_params()))
def test_autopool_pages(fn_name, kwargs):
    _run_autopool_page(**kwargs, _fn_name=fn_name)


# naivily test only USD autopool to see the speed of each page
def _limited_autopool_params():
    for fn_name, _ in AUTOPOOL_CONTENT_FUNCTIONS.items():
        for autopool in [AUTO_USD]:
            yield pytest.param(
                fn_name,
                {"autopool": autopool},
                id=f"autopool-{fn_name}-{autopool.name}",
            )


@pytest.mark.speed
@pytest.mark.parametrize("fn_name,kwargs", list(_limited_autopool_params()))
def test_limited_autopool_pages(fn_name, kwargs):
    _run_autopool_page(**kwargs, _fn_name=fn_name)


def _run_risk_metrics_page(chain, base_asset, valid_autopools, _fn_name: str):
    from mainnet_launch.pages.risk_metrics import RISK_METRICS_FUNCTIONS_WITH_ARGS as _fns

    fn = _fns[_fn_name]
    fn(chain=chain, base_asset=base_asset, valid_autopools=valid_autopools)


def _risk_metrics_params():
    for fn_name, _ in RISK_METRICS_FUNCTIONS_WITH_ARGS.items():
        for (chain, base_asset), valid_autopools in CHAIN_BASE_ASSET_GROUPS.items():
            yield pytest.param(
                fn_name,
                {"chain": chain, "base_asset": base_asset, "valid_autopools": valid_autopools},
                id=f"risk-{fn_name}-{chain.name}-{base_asset.name}",
            )


@pytest.mark.parametrize("fn_name,kwargs", list(_risk_metrics_params()))
def test_risk_metrics_pages(fn_name, kwargs):
    _run_risk_metrics_page(**kwargs, _fn_name=fn_name)
