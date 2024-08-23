from v2_rebalance_dashboard.fetch_lp_composition_over_time import fetch_lp_tokens_and_eth_value_per_destination

fig = fetch_lp_tokens_and_eth_value_per_destination()
fig.show()

from v2_rebalance_dashboard.fetch_asset_combination_over_time import fetch_asset_composition_over_time_to_plot

fig = fetch_asset_composition_over_time_to_plot()
fig.show()


from v2_rebalance_dashboard.fetch_nav_per_share import fetch_daily_nav_per_share_to_plot

fig = fetch_daily_nav_per_share_to_plot()
fig.show()

from v2_rebalance_dashboard.fetch_nav import fetch_daily_nav_to_plot

fig = fetch_daily_nav_to_plot()
fig.show()
