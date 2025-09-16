from .gas_costs.gas_costs import fetch_and_render_gas_costs
from .autopool_fees import fetch_and_render_autopool_fees

#
PROTOCOL_CONTENT_FUNCTIONS = {
    "Gas Costs": fetch_and_render_gas_costs,
    "Autopool Fees": fetch_and_render_autopool_fees,
}

__all__ = ["PROTOCOL_CONTENT_FUNCTIONS"]
