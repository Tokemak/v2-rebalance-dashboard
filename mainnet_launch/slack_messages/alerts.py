import argparse

import requests
from blockkit import Message, MarkdownText
from mainnet_launch.constants import AUTOPOOL_DASHBOARD_UPDATES_SLACK_WEBHOOK_URL


def send_slack_message_via_webhook(
    success: bool,
    action_name: str,
    action_url: str,
):
    emoji = "✅" if success else "❌"
    message = Message(
        blocks=[
            MarkdownText(text=f"{emoji} | {action_name} | [see action]({action_url})"),
        ]
    )

    requests.post(AUTOPOOL_DASHBOARD_UPDATES_SLACK_WEBHOOK_URL, json=message.model_dump_json())


def cli():
    parser = argparse.ArgumentParser(description="Send a Slack message about a GitHub Action result.")
    parser.add_argument("--success", required=True, help="Job status: 'success' or anything else.")
    parser.add_argument("--action-name", required=True, help="GitHub workflow name")
    parser.add_argument("--github-actions-url", required=True, help="The URL to the current github actions run")
    args = parser.parse_args()

    success = args.success.lower() == "success"
    send_slack_message_via_webhook(success, args.action_name, args.github_actions_url)


if __name__ == "__main__":
    cli()
