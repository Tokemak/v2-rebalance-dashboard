import requests
import pandas as pd
from pprint import pprint


from mainnet_launch.constants import V2_DASHBOARD_NOTIFS_WEBHOOK_URL

from dotenv import load_dotenv
import os




from slack_sdk import WebClient


load_dotenv()

SLACK_OAUTH_TOKEN = os.environ.get("SLACK_OAUTH_TOKEN")
TESTING_WEBHOOK_URL = os.environ.get("TESTING_SLACK_WEBHOOK_URL")

TESTING_CHANNEL_ID = 'C09MHUS35V0'



slack_client = WebClient(token=SLACK_OAUTH_TOKEN)
auth_test = slack_client.auth_test()
bot_user_id = auth_test["user_id"]
print(f"App's bot user: {bot_user_id}")

files = client.files_list(user=bot_user_id)

# slack_client.files_u
# # def post_slack_message_with_file(title:str, channel_id:str, df:pd.DataFrame):

    


# def post_slack_message(message: Message):
#     payload = message.build()
#     resp = requests.post(TESTING_WEBHOOK_URL, json=payload, timeout=10)
#     resp.raise_for_status()
#     print(f"Message posted successfully, status code: {resp.status_code}")


# def post_message_with_table(title: str, df: pd.DataFrame):
#     """
#     Post a slack message with a table.

#     Args:
#         title (str): The title of the message.
#         df (pd.DataFrame): The dataframe to post as a table.
#     """
#     table_contents = df.to_markdown(index=False)

#     markdown_table = "```\n" + table_contents + "```\n"

#     message = Message(blocks=[Section(text=MarkdownText(text=title)), Section(text=MarkdownText(text=markdown_table))])

#     post_slack_message(message)
#     pprint(df)
