from mainnet_launch.slack_messages.post_message import SlackChannel, post_slack_message
from mainnet_launch.slack_messages.incentives.no_claimed_expected_incentives import post_missing_balance_updated_events
from mainnet_launch.slack_messages.concentration.high_pool_exposure import post_destination_ownership_exposure_table
from mainnet_launch.slack_messages.concentration.holding_illiquid_tokens import post_illiquid_token_holding_analysis
from mainnet_launch.slack_messages.solver.solver_plans_and_events import post_autopools_without_generated_plans
from mainnet_launch.slack_messages.incentives.not_recently_sold_tokens import post_unsold_incentive_tokens
from mainnet_launch.slack_messages.depegs.asset_depegs import post_asset_depeg_slack_message
from mainnet_launch.slack_messages.new_destinations.get_possible_new_destinations import post_possible_new_destinations


def post_message_or_error_message(fn, slack_channel):
    try:
        fn(slack_channel)
    except Exception as e:
        post_slack_message(slack_channel, f"❌ {fn.__name__=} raised Exception {e}")


def post_daily_messages(slack_channel: SlackChannel = SlackChannel.PRODUCTION):
    """Notifications that signal we should do *something*"""
    for fn in [
        post_autopools_without_generated_plans,
        post_missing_balance_updated_events,
        post_unsold_incentive_tokens,
        post_asset_depeg_slack_message,
    ]:
        post_message_or_error_message(fn, slack_channel)


def post_weekly_messages(slack_channel: SlackChannel = SlackChannel.PRODUCTION):
    """Notifications that about the general state of the autopool, but don't require instant action"""
    for fn in [
        post_destination_ownership_exposure_table,
        post_illiquid_token_holding_analysis,
        post_possible_new_destinations,
    ]:
        post_message_or_error_message(fn, slack_channel)


if __name__ == "__main__":
    post_daily_messages(SlackChannel.CI)
    post_weekly_messages(SlackChannel.TESTING)
