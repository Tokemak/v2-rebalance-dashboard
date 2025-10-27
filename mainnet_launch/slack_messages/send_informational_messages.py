"""Run this to send a bunch of updates to slack about the various doings of the autopool setup"""

from mainnet_launch.constants import profile_function

from mainnet_launch.slack_messages.incentives.no_claimed_expected_incentives import post_missing_balance_updated_events
from mainnet_launch.slack_messages.concentration.high_pool_exposure import post_destination_ownership_exposure_table
from mainnet_launch.slack_messages.solver.solver_plans_and_events import post_autopools_without_generated_plans


def send_information_slack_messages():
    def post_messages():
        post_missing_balance_updated_events()
        post_destination_ownership_exposure_table()
        post_autopools_without_generated_plans()

    profile_function(post_messages)
