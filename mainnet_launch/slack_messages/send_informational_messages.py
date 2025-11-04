"""Run this to send a bunch of updates to slack about the various doings of the autopool setup"""

import datetime

from mainnet_launch.constants import profile_function
from mainnet_launch.slack_messages.post_message import SlackChannel
from mainnet_launch.slack_messages.incentives.no_claimed_expected_incentives import post_missing_balance_updated_events
from mainnet_launch.slack_messages.concentration.high_pool_exposure import post_destination_ownership_exposure_table
from mainnet_launch.slack_messages.concentration.holding_illiquid_tokens import post_illiquid_token_holding_analysis
from mainnet_launch.slack_messages.solver.solver_plans_and_events import post_autopools_without_generated_plans
from mainnet_launch.slack_messages.incentives.not_recently_sold_tokens import post_unsold_incentive_tokens
from mainnet_launch.slack_messages.depegs.asset_depegs import post_asset_depeg_slack_message
from mainnet_launch.slack_messages.post_message import post_slack_message


def send_information_slack_messages():
    def post_messages(slack_channel: SlackChannel):
        post_slack_message(
            slack_channel,
            f"Informational messages run at {datetime.datetime.now().isoformat()}",
        )
        visual_line_break = "-" * 40
        post_slack_message(slack_channel, visual_line_break)
        post_destination_ownership_exposure_table(slack_channel)
        post_autopools_without_generated_plans(slack_channel)
        post_missing_balance_updated_events(slack_channel)
        post_unsold_incentive_tokens(slack_channel)
        post_asset_depeg_slack_message(slack_channel)
        post_illiquid_token_holding_analysis(slack_channel)

        post_slack_message(slack_channel, visual_line_break)

    profile_function(post_messages, SlackChannel.PRODUCTION)


if __name__ == "__main__":
    send_information_slack_messages()
