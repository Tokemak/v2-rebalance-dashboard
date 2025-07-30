from mainnet_launch.pages.risk_metrics.estimate_exit_liquidity_from_quotes import (
    fetch_and_render_exit_liquidity_from_quotes,
)
from mainnet_launch.pages.risk_metrics.percent_ownership_by_destination import (
    fetch_and_render_our_percent_ownership_of_each_destination,
)
from mainnet_launch.pages.risk_metrics.estimate_exit_liquidity_from_pool_tvl import (
    fetch_exit_liqudity_tvl
)

# these should be seperate pages


# TODO make this take a chain an arg
def fetch_and_render_risk_metrics():
    fetch_and_render_exit_liquidity_from_quotes()
    fetch_and_render_our_percent_ownership_of_each_destination()


