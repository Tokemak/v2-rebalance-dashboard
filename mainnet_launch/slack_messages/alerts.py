import argparse

from .post_message import post_slack_message, SlackChannel


def _send_slack_message_about_github_action_status(success: bool, action_name: str, action_url: str):
    emoji = "✅" if success else "❌"
    message = f"{emoji} | {action_name} | <{action_url}|see action logs> "
    post_slack_message(SlackChannel.PRODUCTION, message)


def post_github_action_status():
    parser = argparse.ArgumentParser(description="Send a Slack message about a GitHub Action result.")
    parser.add_argument("--success", required=True, help="Job status: 'success' or anything else.")
    parser.add_argument("--action-name", required=True, help="GitHub workflow name")
    parser.add_argument("--github-actions-url", required=True, help="The URL to the current github actions run")
    args = parser.parse_args()

    success = args.success.lower() == "success"
    _send_slack_message_about_github_action_status(success, args.action_name, args.github_actions_url)
