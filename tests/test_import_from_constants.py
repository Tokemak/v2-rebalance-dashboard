import importlib

from mainnet_launch.pages.risk_metrics.drop_down import render_pick_chain_and_base_asset_dropdown


def test_import_constants():
    importlib.import_module("mainnet_launch.constants")
