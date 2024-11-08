from mainnet_launch.autopool_diagnostics.autopool_diagnostics_tab import (
    fetch_and_render_autopool_diagnostics_data,
    fetch_autopool_diagnostics_data,
)
from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    fetch_destination_allocation_over_time_data,
    fetch_and_render_destination_allocation_over_time_data,
)
from mainnet_launch.destination_diagnostics.weighted_crm import (
    fetch_weighted_crm_data,
    fetch_and_render_weighted_crm_data,
    fetch_and_render_destination_apr_data,
)

from mainnet_launch.solver_diagnostics.rebalance_events import (
    fetch_rebalance_events_data,
    fetch_and_render_rebalance_events_data,
)
from mainnet_launch.solver_diagnostics.solver_diagnostics import (
    fetch_and_render_solver_diagnositics_data,
    fetch_solver_diagnostics_data,
)

from mainnet_launch.top_level.key_metrics import fetch_key_metrics_data, fetch_and_render_key_metrics_data
from mainnet_launch.gas_costs.keeper_network_gas_costs import (
    fetch_keeper_network_gas_costs,
    fetch_and_render_keeper_network_gas_costs,
)

from mainnet_launch.accounting.incentive_token_liqudiation_prices import (
    fetch_reward_token_achieved_vs_incentive_token_price,
    fetch_and_render_reward_token_achieved_vs_incentive_token_price,
)

from mainnet_launch.accounting.protocol_level_profit import (
    fetch_protocol_level_profit_and_loss_data,
    fetch_and_render_protocol_level_profit_and_loss_data,
)


from mainnet_launch.constants import AutopoolConstants


PER_AUTOPOOOL_DATA_CACHING_FUNCTIONS = [
    fetch_solver_diagnostics_data,
    fetch_key_metrics_data,
    fetch_autopool_diagnostics_data,
    fetch_destination_allocation_over_time_data,
    fetch_weighted_crm_data,
    fetch_rebalance_events_data,
]

NOT_PER_AUTOPOOL_DATA_CACHING_FUNCTIONS = [
    fetch_keeper_network_gas_costs,
    fetch_reward_token_achieved_vs_incentive_token_price,
    fetch_protocol_level_profit_and_loss_data,
]


def display_destination_diagnostics(autopool: AutopoolConstants):
    fetch_and_render_destination_apr_data(autopool)
    # a chart of

    # composite return out

    # composite retun in

    # price, fee, incentive points points
    # for all the destinations


CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Exposure": fetch_and_render_destination_allocation_over_time_data,
    "Autopool CRM": fetch_and_render_weighted_crm_data,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
    "Autopool Diagnostics": fetch_and_render_autopool_diagnostics_data,
    "Destination Diagnostics": display_destination_diagnostics,
    "Solver Diagnostics": fetch_and_render_solver_diagnositics_data,
    "Gas Costs": fetch_and_render_keeper_network_gas_costs,
    "Incentive Token Prices": fetch_and_render_reward_token_achieved_vs_incentive_token_price,
    "Protocol Level Profit and Loss": fetch_and_render_protocol_level_profit_and_loss_data,
}

PAGES_WITHOUT_AUTOPOOL = ["Gas Costs", "Incentive Token Prices", "Protocol Level Profit and Loss"]
