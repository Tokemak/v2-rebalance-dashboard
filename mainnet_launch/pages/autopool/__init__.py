from .autopool_exposure.allocation_over_time import fetch_and_render_asset_allocation_over_time
from .autopool_crm.weighted_crm import fetch_and_render_weighted_crm_data
from .destination_diagnostics.destination_diagnostics import fetch_and_render_destination_apr_data
from .rebalance_events.rebalance_events import fetch_and_render_rebalance_events_data
from .solver_diagnostics.solver_diagnostics import fetch_and_render_solver_diagnostics_data
from .key_metrics.key_metrics import fetch_and_render_key_metrics_data
from .asset_discounts.fetch_and_render_asset_discounts import fetch_and_render_asset_discounts
from .autopool_deposits_and_withdrawals.render_autopool_deposits_and_withdrawals import (
    fetch_and_render_autopool_deposits_and_withdrawals,
)

AUTOPOOL_CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Exposure": fetch_and_render_asset_allocation_over_time,
    "Autopool CRM": fetch_and_render_weighted_crm_data,
    "Destination Diagnostics": fetch_and_render_destination_apr_data,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
    "Asset Discounts": fetch_and_render_asset_discounts,
    "Solver Diagnostics": fetch_and_render_solver_diagnostics_data,
    "User Deposits And Withdrawals": fetch_and_render_autopool_deposits_and_withdrawals,
}

__all__ = ["AUTOPOOL_CONTENT_FUNCTIONS"]
