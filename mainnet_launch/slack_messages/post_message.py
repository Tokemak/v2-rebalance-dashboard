from mainnet_launch.constants import V2_DASHBOARD_NOTIFS_WEBHOOK_URL

import requests
import pandas as pd


from blockkit import Message


def post_slack_message(message: Message):
    payload = message.build()
    resp = requests.post(V2_DASHBOARD_NOTIFS_WEBHOOK_URL, json=payload, timeout=10)
    resp.raise_for_status()


def post_message_with_table(title: str, df: pd.DataFrame):
    """
    Post a slack message with a table.

    Args:
        title (str): The title of the message.
        df (pd.DataFrame): The dataframe to post as a table.
    """
    markdown_table = df.to_markdown(index=False)

    message = Message(
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*\n```{markdown_table}```"}}]
    )

    post_slack_message(message)
