from mainnet_launch.slack_messages.post_message import SlackChannel
from mainnet_launch.slack_messages import post_daily_messages, post_weekly_messages


def test_daily_slack_messages(capsys):
    post_daily_messages(SlackChannel.CI)
    out = capsys.readouterr().out
    assert "[CI SLACK MESSAGE]" in out


def test_weekly_slack_messages(capsys):
    post_weekly_messages(SlackChannel.CI)
    out = capsys.readouterr().out
    assert "[CI SLACK MESSAGE]" in out


if __name__ == "__main__":
    post_daily_messages(SlackChannel.CI)
    post_weekly_messages(SlackChannel.CI)
