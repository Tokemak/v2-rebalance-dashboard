from mainnet_launch.autopool_diagnostics.autopool_diagnostics_tab import (
    fetch_and_render_autopool_diagnostics_data,
)
from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    fetch_destination_allocation_over_time_data,
    fetch_and_render_destination_allocation_over_time_data,
)
from mainnet_launch.destination_diagnostics.weighted_crm import (
    fetch_and_render_weighted_crm_data,
    fetch_and_render_destination_apr_data,
)

from mainnet_launch.solver_diagnostics.rebalance_events import (
    fetch_and_render_rebalance_events_data,
)
from mainnet_launch.solver_diagnostics.solver_diagnostics import (
    fetch_and_render_solver_diagnositics_data,
)

from mainnet_launch.top_level.key_metrics import fetch_and_render_key_metrics_data
from mainnet_launch.gas_costs.keeper_network_gas_costs import (
    fetch_and_render_keeper_network_gas_costs,
)

from mainnet_launch.accounting.incentive_token_liqudiation_prices import (
    fetch_and_render_reward_token_achieved_vs_incentive_token_price,
)

from mainnet_launch.accounting.protocol_level_profit import (
    fetch_and_render_protocol_level_profit_and_loss_data,
)


from mainnet_launch.constants import AutopoolConstants


def display_destination_diagnostics(autopool: AutopoolConstants):
    fetch_and_render_destination_apr_data(autopool)
    # TODO add a chart of

    # composite return out

    # composite return in

    # price, fee, incentive points
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
