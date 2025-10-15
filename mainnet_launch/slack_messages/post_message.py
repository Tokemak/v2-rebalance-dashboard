from mainnet_launch.constants import V2_DASHBOARD_NOTIFS_WEBHOOK_URL

import requests
import pandas as pd
from pprint import pprint


from blockkit import Message, Section, MarkdownText


def post_slack_message(message: Message):
    payload = message.build()
    resp = requests.post(V2_DASHBOARD_NOTIFS_WEBHOOK_URL, json=payload, timeout=10)
    resp.raise_for_status()
    print(f"Message posted successfully, status code: {resp.status_code}")
    pprint(message.build())


def post_message_with_table(title: str, df: pd.DataFrame):
    """
    Post a slack message with a table.

    Args:
        title (str): The title of the message.
        df (pd.DataFrame): The dataframe to post as a table.
    """
    markdown_table = f"```{df.to_markdown(index=False)}```"

    message = Message(blocks=[Section(text=MarkdownText(text=title)), Section(text=MarkdownText(text=markdown_table))])
    post_slack_message(message)
