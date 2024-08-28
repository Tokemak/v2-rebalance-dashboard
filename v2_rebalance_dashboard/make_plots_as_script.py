from v2_rebalance_dashboard.fetch_destination_summary_stats import fetch_summary_stats_figures
from v2_rebalance_dashboard.fetch_asset_combination_over_time import fetch_asset_composition_over_time_to_plot
from v2_rebalance_dashboard.fetch_nav_per_share import fetch_daily_nav_per_share_to_plot
from v2_rebalance_dashboard.constants import eth_client


def get_autopool_diagnostics_charts(autopool_name="balETH"):
    if autopool_name != "balETH":
        raise ValueError("only works for balETH autopool")

    eth_allocation_bar_chart_fig, composite_return_out_fig, current_allocation_pie_fig = fetch_summary_stats_figures()
    nav_per_share_fig = fetch_daily_nav_per_share_to_plot()
    asset_allocation_bar_fig, asset_allocation_pie_fig = fetch_asset_composition_over_time_to_plot()

    return {
        "eth_allocation_bar_chart_fig": eth_allocation_bar_chart_fig,
        "composite_return_out_fig": composite_return_out_fig,
        "current_allocation_pie_fig": current_allocation_pie_fig,
        "nav_per_share_fig": nav_per_share_fig,
        "asset_allocation_bar_fig": asset_allocation_bar_fig,
        "asset_allocation_pie_fig": asset_allocation_pie_fig,
    }
