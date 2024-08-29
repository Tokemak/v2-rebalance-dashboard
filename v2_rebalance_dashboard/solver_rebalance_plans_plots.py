import json
import xml.etree.ElementTree as ET
import requests
import pandas as pd
from v2_rebalance_dashboard.constants import ROOT_DIR
from v2_rebalance_dashboard.get_rebalance_events_summary import fetch_clean_rebalance_events

GET_REBALANCE_PLAN_FILE_NAMES_URL = "https://ctrrwpvz5c.execute-api.us-east-1.amazonaws.com/GuardedLaunch/files"
fetched_data_path = ROOT_DIR.parent / "fetched_data"


def fetch_s3_contents_to_dataframe(url):
    # Send the GET request
    files = requests.get(url)

    # Parse the XML content
    tree = ET.ElementTree(ET.fromstring(files.content))
    root = tree.getroot()

    # Define the namespace
    namespace = {"ns": "http://s3.amazonaws.com/doc/2006-03-01/"}

    # Initialize the list to hold the contents
    contents_list = []

    # Extract the contents
    for content in root.findall("ns:Contents", namespace):
        item = {
            "Key": content.find("ns:Key", namespace).text,
            "LastModified": content.find("ns:LastModified", namespace).text,
            "ETag": content.find("ns:ETag", namespace).text.replace('"', ""),
            "Size": int(content.find("ns:Size", namespace).text),
            "StorageClass": content.find("ns:StorageClass", namespace).text,
        }
        contents_list.append(item)

    # Convert the list of contents to a DataFrame
    df = pd.DataFrame(contents_list)

    return df


def _ensure_all_rebalance_plans_are_loaded():
    df = fetch_s3_contents_to_dataframe(GET_REBALANCE_PLAN_FILE_NAMES_URL)

    existing_jsons = [str(path).split("/")[-1] for path in fetched_data_path.glob("*.json")]
    jsons_to_fetch = [json_path for json_path in df["Key"] if json_path not in existing_jsons]

    print(f"{len(jsons_to_fetch)=}", f"{len(existing_jsons)=}")

    for json_key in jsons_to_fetch:
        try:
            json_data = requests.get(
                f"https://ctrrwpvz5c.execute-api.us-east-1.amazonaws.com/GuardedLaunch/files/{json_key}"
            )

            with open(fetched_data_path / json_key, "w") as fout:
                json.dump(json.loads(json_data.content), fout, indent=4)
                # print("wrote", str(json_key))
        except Exception as e:
            print(e, type(e), json_key)


def fetch_rebalance_plans_plots(autopool_name: str = "balETH"):
    if autopool_name != "balETH":
        raise ValueError("only works for balETH")
    _ensure_all_rebalance_plans_are_loaded()

    existing_jsons = [str(path).split("/")[-1] for path in fetched_data_path.glob("*.json")]
    clean_rebalance_df = fetch_clean_rebalance_events()
