from mainnet_launch.slack_messages.post_message import SlackChannel
from mainnet_launch.slack_messages import post_daily_messages, post_weekly_messages


def test_daily_slack_messages():
    post_daily_messages(SlackChannel.CI)

def test_weekly_slack_messages():   
    post_weekly_messages(SlackChannel.CI)
