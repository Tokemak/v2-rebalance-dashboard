"""Run this to send a bunch of updates to slack about the various doings of the autopool setup"""

from mainnet_launch.slack_messages.post_message import SlackChannel, post_slack_message
from mainnet_launch.slack_messages.incentives.no_claimed_expected_incentives import post_missing_balance_updated_events
from mainnet_launch.slack_messages.concentration.high_pool_exposure import post_destination_ownership_exposure_table
from mainnet_launch.slack_messages.solver.solver_plans_and_events import post_autopools_without_generated_plans

from mainnet_launch.slack_messages.incentives.not_recently_sold_tokens import post_unsold_incentive_tokens


def test_slack_messages():
    slack_channel = SlackChannel.TESTING
    post_destination_ownership_exposure_table(slack_channel)
    post_autopools_without_generated_plans(slack_channel)
    post_missing_balance_updated_events(slack_channel)
    post_unsold_incentive_tokens(slack_channel)
