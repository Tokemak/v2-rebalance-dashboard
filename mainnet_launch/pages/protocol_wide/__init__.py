from .gas_costs.gas_costs import fetch_and_render_gas_costs

# from .protocol_level_profit_and_loss.fees import fetch_and_render_fees  # Optional future import
# from .protocol_level_profit_and_loss.protocol_level_profit import fetch_and_render_protocol_level_profit  # Optional future import

PROTOCOL_CONTENT_FUNCTIONS = {
    "Gas Costs": fetch_and_render_gas_costs,
    # "Fees": fetch_and_render_fees,
    # "Protocol Level Profit": fetch_and_render_protocol_level_profit,
}

__all__ = [
    "fetch_and_render_gas_costs",
    # "fetch_and_render_fees",
    # "fetch_and_render_protocol_level_profit",
    "PROTOCOL_CONTENT_FUNCTIONS",
]
