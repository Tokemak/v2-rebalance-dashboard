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
    fetch_and_render_solver_diagnositics_data,
)

from mainnet_launch.pages.key_metrics.key_metrics import fetch_and_render_key_metrics_data
from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import (
    fetch_and_render_keeper_network_gas_costs,
)

from mainnet_launch.pages.incentive_token_prices.incentive_token_liqudiation_prices import (
    fetch_and_render_reward_token_achieved_vs_incentive_token_price,
)

from mainnet_launch.pages.protocol_level_profit_and_loss.protocol_level_profit import (
    fetch_and_render_protocol_level_profit_and_loss_data,
)

from mainnet_launch.pages.expected_vs_actual_returns.expected_vs_actual_returns import (
    fetch_and_render_actual_and_gross_and_projected_returns,
)


from mainnet_launch.pages.asset_discounts.fetch_and_render_asset_discounts import (
    fetch_and_render_asset_discounts,
)


# consider moving this elsewhere

CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Exposure": fetch_and_render_asset_allocation_over_time,
    "Autopool CRM": fetch_and_render_weighted_crm_data,
    "Destination Diagnostics": fetch_and_render_destination_apr_data,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
    "Asset Discounts": fetch_and_render_asset_discounts,
    # "Autopool Diagnostics": fetch_and_render_autopool_diagnostics_data,
    # "Solver Diagnostics": fetch_and_render_solver_diagnositics_data,
    # "Expected and Actual Returns": fetch_and_render_actual_and_gross_and_projected_returns,
    # "Gas Costs": fetch_and_render_keeper_network_gas_costs,
    # "Incentive Token Prices": fetch_and_render_reward_token_achieved_vs_incentive_token_price,
    # "Protocol Level Profit and Loss": fetch_and_render_protocol_level_profit_and_loss_data,
    # "Asset Discounts": fetch_and_render_asset_oracle_and_backing,
}

PAGES_WITHOUT_AUTOPOOL = (
    []
)  # ["Gas Costs", "Incentive Token Prices", "Protocol Level Profit and Loss", "Asset Discounts"]
