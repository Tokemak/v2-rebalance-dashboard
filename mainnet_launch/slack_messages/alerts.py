import argparse


from blockkit import Message, MarkdownText, Section
import requests


from mainnet_launch.constants import V2_DASHBOARD_NOTIFS_WEBHOOK_URL


def send_slack_message_about_github_action_status(success: bool, action_name: str, action_url: str):
    emoji = "✅" if success else "❌"

    payload = Message(
        blocks=[Section(text=MarkdownText(text=f"{emoji} | {action_name} | <{action_url}| see action> "))]
    ).build()

    resp = requests.post(V2_DASHBOARD_NOTIFS_WEBHOOK_URL, json=payload, timeout=10)
    # We want a run time failure if this fails to see in the github action logs
    resp.raise_for_status()


def cli():
    parser = argparse.ArgumentParser(description="Send a Slack message about a GitHub Action result.")
    parser.add_argument("--success", required=True, help="Job status: 'success' or anything else.")
    parser.add_argument("--action-name", required=True, help="GitHub workflow name")
    parser.add_argument("--github-actions-url", required=True, help="The URL to the current github actions run")
    args = parser.parse_args()

    success = args.success.lower() == "success"
    send_slack_message_about_github_action_status(success, args.action_name, args.github_actions_url)
