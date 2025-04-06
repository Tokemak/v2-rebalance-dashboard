from mainnet_launch.pages.protocol_level_profit_and_loss.fees import (
    fetch_and_render_autopool_fee_data,
)
from mainnet_launch.pages.protocol_level_profit_and_loss.debt_reporting import fetch_and_render_autopool_rewardliq_plot
from mainnet_launch.pages.autopool_diagnostics.deposits_and_withdrawals import (
    fetch_and_render_autopool_deposit_and_withdraw_stats_data,
)
from mainnet_launch.pages.autopool_diagnostics.count_of_destinations import (
    fetch_and_render_autopool_destination_counts_data,
)
from mainnet_launch.pages.autopool_diagnostics.turnover import fetch_and_render_turnover_data

from mainnet_launch.pages.autopool_diagnostics.returns_before_expenses import (
    fetch_and_render_autopool_return_and_expenses_metrics,
)
from mainnet_launch.constants import AutopoolConstants


def fetch_and_render_autopool_diagnostics_data(autopool: AutopoolConstants):
    fetch_and_render_autopool_fee_data(autopool)  # can't test until it has fees
    fetch_and_render_turnover_data(autopool)  # depends on rebalance events
    fetch_and_render_autopool_deposit_and_withdraw_stats_data(autopool)  # works updated decimals
    fetch_and_render_autopool_destination_counts_data(autopool)  # depends on get destination summary stats
    fetch_and_render_autopool_return_and_expenses_metrics(autopool)  # depends on rebalance events
    fetch_and_render_autopool_rewardliq_plot(autopool)  # not certain, claimed = 0, follow up when there is more info


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_ETH

    fetch_and_render_autopool_diagnostics_data(AUTO_ETH)
