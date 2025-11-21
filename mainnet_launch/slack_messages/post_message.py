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

RED_CIRCLE = "🔴"
GREEN_CIRCLE = "🟢"
YELLOW_CIRCLE = "🟡"


class SlackChannel(Enum):
    TESTING = TESTING_CHANNEL_ID
    PRODUCTION = PRODUCTION_CHANNEL_ID
    CI = "CI"  # dummy for printing to stdout instead of posting to Slack


def post_slack_message(channel: SlackChannel, text: str) -> None:
    if channel != SlackChannel.CI:
        slack_client.chat_postMessage(channel=channel.value, text=text)
    else:
        print(f"[CI SLACK MESSAGE] {text}\n")


def post_message_with_table(
    channel: SlackChannel, initial_comment: str, df: pd.DataFrame, file_save_name: str, show_index=False
) -> None:
    table_csv = df.to_csv(index=show_index)

    if channel != SlackChannel.CI:
        slack_client.files_upload_v2(
            channel=channel.value,
            filename=file_save_name,
            initial_comment=initial_comment,
            content=table_csv,
        )
    else:
        print(f"[CI SLACK MESSAGE] {initial_comment}\n{table_csv}\n\n")
