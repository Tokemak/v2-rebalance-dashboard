from mainnet_launch.pages.autopool_diagnostics.autopool_diagnostics_tab import (
    fetch_and_render_autopool_diagnostics_data,
)
from mainnet_launch.pages.autopool_exposure.allocation_over_time import (
    fetch_and_render_asset_allocation_over_time,
)
from mainnet_launch.pages.autopool_crm.weighted_crm import (
    fetch_and_render_weighted_crm_data,
)

from mainnet_launch.pages.destination_diagnostics.destination_diagnostics import fetch_and_render_destination_apr_data

from mainnet_launch.pages.rebalance_events.rebalance_events import (
    fetch_and_render_rebalance_events_data,
)

from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import (
    fetch_and_render_solver_diagnostics_data,
)

from mainnet_launch.pages.key_metrics.key_metrics import fetch_and_render_key_metrics_data
from mainnet_launch.pages.gas_costs.gas_costs import (
    fetch_and_render_gas_costs,
)

from mainnet_launch.pages.asset_discounts.fetch_and_render_asset_discounts import (
    fetch_and_render_asset_discounts,
)

from mainnet_launch.pages.risk_metrics.percent_ownership_by_destination import (
    fetch_and_render_our_percent_ownership_of_each_destination,
)

# from mainnet_launch.pages.risk_metrics.estimate_exit_liquidity_from_pool_tvl import fetch_and_render_exit_liqudity_pools

from mainnet_launch.pages.risk_metrics.render_exit_liquidity_batch import (
    fetch_and_render_exit_liquidity_from_quotes,
)

from mainnet_launch.app.marketing_app.marketing_pages.autopool_cumulative_volume import (
    fetch_and_render_cumulative_volume,
)

from mainnet_launch.app.marketing_app.marketing_pages.apr_and_tvl_by_destination_script import (
    fetch_and_render_autopool_apy_and_allocation_over_time,
)


AUTOPOOL_CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Exposure": fetch_and_render_asset_allocation_over_time,
    "Autopool CRM": fetch_and_render_weighted_crm_data,
    "Destination Diagnostics": fetch_and_render_destination_apr_data,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
    "Asset Discounts": fetch_and_render_asset_discounts,
    "Solver Diagnostics": fetch_and_render_solver_diagnostics_data,
}


PROTOCOL_CONTENT_FUNCTIONS = {
    "Gas Costs": fetch_and_render_gas_costs,
    # proift and loss
}

RISK_METRICS_FUNCTIONS = {
    "Tokemak Percent Ownership": fetch_and_render_our_percent_ownership_of_each_destination,
    # "Exit Liquidity Pools": fetch_and_render_exit_liqudity_pools,
    "Exit Liquidity Quotes": fetch_and_render_exit_liquidity_from_quotes,
}

MARKETING_CONTENT_FUNCTIONS = {
    "Cumulative Volume": fetch_and_render_cumulative_volume,
    "APY and Allocation Over Time": fetch_and_render_autopool_apy_and_allocation_over_time,
}
