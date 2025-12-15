from mainnet_launch.slack_messages.post_message import SlackChannel
from mainnet_launch.slack_messages.incentives.no_claimed_expected_incentives import post_missing_balance_updated_events
from mainnet_launch.slack_messages.concentration.high_pool_exposure import post_destination_ownership_exposure_table
from mainnet_launch.slack_messages.concentration.holding_illiquid_tokens import post_illiquid_token_holding_analysis
from mainnet_launch.slack_messages.solver.solver_plans_and_events import post_autopools_without_generated_plans
from mainnet_launch.slack_messages.incentives.not_recently_sold_tokens import post_unsold_incentive_tokens
from mainnet_launch.slack_messages.depegs.asset_depegs import post_asset_depeg_slack_message
from mainnet_launch.slack_messages.new_destinations.get_possible_new_destinations import post_possible_new_destinations


def post_daily_messages(slack_channel: SlackChannel = SlackChannel.PRODUCTION):
    """Notifications that signal we should do *something*"""
    post_autopools_without_generated_plans(slack_channel)
    post_missing_balance_updated_events(slack_channel)
    post_unsold_incentive_tokens(slack_channel)
    post_asset_depeg_slack_message(slack_channel)


def post_weekly_messages(slack_channel: SlackChannel = SlackChannel.PRODUCTION):
    """Notifications that about the general state of the autopool, but don't require instant action"""
    post_destination_ownership_exposure_table(slack_channel)

    post_illiquid_token_holding_analysis(slack_channel)
    post_possible_new_destinations(slack_channel)
    pass


if __name__ == "__main__":
    post_weekly_messages(SlackChannel.TESTING)
    pass
