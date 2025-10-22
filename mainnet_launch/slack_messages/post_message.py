import requests
import pandas as pd
from pprint import pprint
from blockkit import Message, Section, MarkdownText


from mainnet_launch.constants import V2_DASHBOARD_NOTIFS_WEBHOOK_URL

from dotenv import load_dotenv
import os

load_dotenv()

TESTING_WEBHOOK_URL = os.environ.get("TESTING_SLACK_WEBHOOK_URL")


def post_slack_message(message: Message):
    payload = message.build()
    resp = requests.post(TESTING_WEBHOOK_URL, json=payload, timeout=10)
    resp.raise_for_status()
    print(f"Message posted successfully, status code: {resp.status_code}")


def post_message_with_table(title: str, df: pd.DataFrame):
    """
    Post a slack message with a table.

    Args:
        title (str): The title of the message.
        df (pd.DataFrame): The dataframe to post as a table.
    """
    table_contents = df.to_markdown(index=False)

    markdown_table = "```\n" + table_contents + "```\n"

    message = Message(blocks=[Section(text=MarkdownText(text=title)), Section(text=MarkdownText(text=markdown_table))])

    post_slack_message(message)
    pprint(df)
