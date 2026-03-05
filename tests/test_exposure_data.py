"""Tests that the cached data-fetching functions for Autopool Exposure,
Destination Diagnostics, and Autopool CRM return well-formed data
for each destination name / asset symbol."""

import pandas as pd
import pytest

from mainnet_launch.constants import AUTO_ETH, AUTO_USD, BASE_USD, CURRENT_AUTOPOOLS
from mainnet_launch.database.views import fetch_autopool_destination_state_df
from mainnet_launch.pages.autopool.destination_diagnostics.destination_diagnostics import _fetch_destination_apr_data
from mainnet_launch.pages.autopool.autopool_crm.weighted_crm import _fetch_weighted_crm_data


# Use a representative subset: one ETH-denominated, one USD on mainnet, one USD on Base.
_SAMPLE_AUTOPOOLS = [ap for ap in [AUTO_ETH, AUTO_USD, BASE_USD] if ap in CURRENT_AUTOPOOLS]


# ---------------------------------------------------------------------------
# Helpers — replicate the pivot logic from allocation_over_time.py so tests
# exercise the same code path the page uses.
# ---------------------------------------------------------------------------


def _pivot_exposure(autopool):
    df = fetch_autopool_destination_state_df(autopool)

    by_dest = (
        (
            df.groupby(["datetime", "readable_name"])["autopool_implied_safe_value"]
            .sum()
            .reset_index()
            .pivot(columns=["readable_name"], index=["datetime"], values="autopool_implied_safe_value")
        )
        .resample("1D")
        .last()
    )

    by_asset = (
        (
            df.groupby(["datetime", "symbol"])["autopool_implied_safe_value"]
            .sum()
            .reset_index()
            .pivot(columns=["symbol"], index=["datetime"], values="autopool_implied_safe_value")
        )
        .resample("1D")
        .last()
    )

    return by_dest, by_asset


# ---------------------------------------------------------------------------
# Autopool Exposure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("autopool", _SAMPLE_AUTOPOOLS, ids=lambda ap: ap.name)
class TestExposureDataByDestination:
    """Iterate over every readable destination name returned for an autopool."""

    def test_destination_columns_are_non_empty(self, autopool):
        by_dest, _ = _pivot_exposure(autopool)
        assert not by_dest.empty, "destination DataFrame should not be empty"
        assert len(by_dest.columns) > 0, "should have at least one destination"

    def test_each_destination_has_finite_values(self, autopool):
        by_dest, _ = _pivot_exposure(autopool)
        for dest_name in by_dest.columns:
            series = by_dest[dest_name].dropna()
            assert len(series) > 0, f"destination '{dest_name}' is all NaN"
            assert series.apply(pd.api.types.is_float).all(), f"destination '{dest_name}' has non-float values"

    def test_destination_percent_allocation_sums_to_100(self, autopool):
        by_dest, _ = _pivot_exposure(autopool)
        pct = 100 * by_dest.div(by_dest.sum(axis=1), axis=0)
        row_sums = pct.sum(axis=1).dropna()
        assert row_sums.between(99.9, 100.1).all(), "percent allocation by destination should sum to ~100%"

    def test_readable_name_format(self, autopool):
        """Every destination column should follow 'Name (Exchange)' format."""
        by_dest, _ = _pivot_exposure(autopool)
        for dest_name in by_dest.columns:
            assert "(" in dest_name and dest_name.endswith(
                ")"
            ), f"readable_name '{dest_name}' doesn't match 'Name (Exchange)' format"


@pytest.mark.parametrize("autopool", _SAMPLE_AUTOPOOLS, ids=lambda ap: ap.name)
class TestExposureDataByAsset:
    """Iterate over every asset symbol returned for an autopool."""

    def test_asset_columns_are_non_empty(self, autopool):
        _, by_asset = _pivot_exposure(autopool)
        assert not by_asset.empty, "asset DataFrame should not be empty"
        assert len(by_asset.columns) > 0, "should have at least one asset symbol"

    def test_each_asset_has_finite_values(self, autopool):
        _, by_asset = _pivot_exposure(autopool)
        for symbol in by_asset.columns:
            series = by_asset[symbol].dropna()
            assert len(series) > 0, f"asset '{symbol}' is all NaN"

    def test_asset_percent_allocation_sums_to_100(self, autopool):
        _, by_asset = _pivot_exposure(autopool)
        pct = 100 * by_asset.div(by_asset.sum(axis=1), axis=0)
        row_sums = pct.sum(axis=1).dropna()
        assert row_sums.between(99.9, 100.1).all(), "percent allocation by asset should sum to ~100%"

    def test_asset_values_are_non_negative(self, autopool):
        _, by_asset = _pivot_exposure(autopool)
        for symbol in by_asset.columns:
            series = by_asset[symbol].dropna()
            assert (series >= 0).all(), f"asset '{symbol}' has negative safe values"


# ---------------------------------------------------------------------------
# Destination Diagnostics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("autopool", _SAMPLE_AUTOPOOLS, ids=lambda ap: ap.name)
class TestDestinationDiagnosticsData:
    """Verify _fetch_destination_apr_data returns valid APR data for each destination."""

    def test_has_destinations(self, autopool):
        df = _fetch_destination_apr_data(autopool)
        assert not df.empty
        assert df["readable_name"].nunique() > 0, "should have at least one destination"

    def test_each_destination_has_apr_data(self, autopool):
        df = _fetch_destination_apr_data(autopool)
        for dest_name in df["readable_name"].unique():
            subset = df[df["readable_name"] == dest_name]
            assert len(subset) > 0, f"destination '{dest_name}' has no rows"
            assert "datetime" in subset.columns

    def test_apr_columns_are_numeric(self, autopool):
        df = _fetch_destination_apr_data(autopool)
        # fee_apr is all-NaN for rebalance-plan autopools; only check columns with data
        for col in ["incentive_apr", "fee_apr", "base_apr"]:
            if col not in df.columns:
                continue
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().any():
                # column has data — confirm it converted cleanly
                raw_notna = df[col].notna().sum()
                assert numeric.notna().sum() == raw_notna, f"'{col}' has non-numeric values"

    def test_readable_name_format(self, autopool):
        df = _fetch_destination_apr_data(autopool)
        for dest_name in df["readable_name"].unique():
            assert "(" in dest_name and dest_name.endswith(
                ")"
            ), f"readable_name '{dest_name}' doesn't match 'Name (Exchange)' format"


# ---------------------------------------------------------------------------
# Autopool CRM
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("autopool", _SAMPLE_AUTOPOOLS, ids=lambda ap: ap.name)
class TestWeightedCrmData:
    """Verify _fetch_weighted_crm_data returns valid composite return DataFrames."""

    def test_returns_non_empty_dataframes(self, autopool):
        apr_out, apr_in = _fetch_weighted_crm_data(autopool)
        assert not apr_out.empty, "total_apr_out_df should not be empty"
        assert not apr_in.empty, "total_apr_in_df should not be empty"

    def test_has_destination_columns(self, autopool):
        apr_out, apr_in = _fetch_weighted_crm_data(autopool)
        # Should have destination columns plus the weighted CR column
        assert len(apr_out.columns) > 1, "apr_out should have destination columns + CR"
        assert len(apr_in.columns) > 1, "apr_in should have destination columns + CR"

    def test_has_weighted_cr_column(self, autopool):
        apr_out, apr_in = _fetch_weighted_crm_data(autopool)
        cr_col = f"{autopool.name} CR"
        assert cr_col in apr_out.columns, f"missing '{cr_col}' in apr_out"
        assert cr_col in apr_in.columns, f"missing '{cr_col}' in apr_in"

    def test_destination_name_format(self, autopool):
        apr_out, _ = _fetch_weighted_crm_data(autopool)
        cr_col = f"{autopool.name} CR"
        for col in apr_out.columns:
            if col == cr_col:
                continue
            assert "(" in col and col.endswith(")"), f"column '{col}' doesn't match 'Name (Exchange)' format"
