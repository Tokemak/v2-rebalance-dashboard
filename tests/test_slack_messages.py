"""Run this to send a bunch of updates to slack about the various doings of the autopool setup"""

from mainnet_launch.constants import profile_function
from mainnet_launch.slack_messages.post_message import SlackChannel, post_slack_message
from mainnet_launch.slack_messages.incentives.no_claimed_expected_incentives import post_missing_balance_updated_events
from mainnet_launch.slack_messages.concentration.high_pool_exposure import post_destination_ownership_exposure_table
from mainnet_launch.slack_messages.solver.solver_plans_and_events import post_autopools_without_generated_plans
import time


def test_slack_messages():
    # not certain about rate limits, ~ message / second
    start_time = time.time()
    post_missing_balance_updated_events(SlackChannel.TESTING)
    post_destination_ownership_exposure_table(SlackChannel.TESTING)
    post_autopools_without_generated_plans(SlackChannel.TESTING)
    end_time = time.time()

    time_taken = end_time - start_time
    post_slack_message(
        SlackChannel.TESTING,
        f"Test slack messages completed in {time_taken:.2f} seconds",
    )
