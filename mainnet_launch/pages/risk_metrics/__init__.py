from .percent_ownership_by_destination import (
    fetch_and_render_our_percent_ownership_of_each_destination,
    fetch_and_render_one_option_for_percent_ownership_by_destination,
)
from .render_exit_liquidity_batch import fetch_and_render_exit_liquidity_from_quotes
from .incentive_token_prices_acutal_vs_expected import render_actual_vs_expected_incentive_token_prices


# from .works_in_progress.estimate_exit_liquidity_from_pool_tvl import fetch_and_render_exit_liqudity_pools  # Optional WIP import

RISK_METRICS_FUNCTIONS = {
    "Tokemak Percent Ownership": fetch_and_render_our_percent_ownership_of_each_destination,
    # "Exit Liquidity Pools": fetch_and_render_exit_liqudity_pools,  # Optional WIP mapping
    "Exit Liquidity Quotes": fetch_and_render_exit_liquidity_from_quotes,
    "Incentive Token Sales (Actual and Expected Prices)": render_actual_vs_expected_incentive_token_prices,
}


RISK_METRICS_FUNCTIONS_WITH_ARGS = {
    "Tokemak Percent Ownership": fetch_and_render_one_option_for_percent_ownership_by_destination,
}


__all__ = ["RISK_METRICS_FUNCTIONS", "RISK_METRICS_FUNCTIONS_WITH_ARGS"]
