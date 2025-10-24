from enum import Enum
import os

import pandas as pd
from slack_sdk import WebClient

from dotenv import load_dotenv


load_dotenv()


SLACK_OAUTH_TOKEN = os.environ.get("SLACK_OAUTH_TOKEN")
slack_client = WebClient(token=SLACK_OAUTH_TOKEN)

TESTING_CHANNEL_ID = "C09MHUS35V0"
PRODUCTION_CHANNEL_ID = "C09JUJDJYQH"


class SlackChannel(Enum):
    TESTING = TESTING_CHANNEL_ID
    PRODUCTION = PRODUCTION_CHANNEL_ID


def post_slack_message(channel: SlackChannel, text: str):
    slack_client.chat_postMessage(channel=channel.value, text=text)


def post_message_with_table(channel: SlackChannel, initial_comment: str, df: pd.DataFrame, df_name: str):
    table_csv = df.to_csv(index=True)
    slack_client.files_upload_v2(
        channel=channel.value,
        filename=df_name,
        initial_comment=initial_comment,
        content=table_csv,
    )
