import json
import os
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


from pathlib import Path

# need a new link
SOLVER_REBALANCE_PLAN_FILES_URL = "https://ctrrwpvz5c.execute-api.us-east-1.amazonaws.com/GuardedLaunch/files"
SOLVER_PLAN_DATA_PATH = Path(__file__).parent / "solver_data_plans"


def ensure_all_rebalance_plans_are_loaded():
    if not os.path.exists(SOLVER_PLAN_DATA_PATH):
        os.mkdir(SOLVER_PLAN_DATA_PATH)

    df = _fetch_s3_contents_to_dataframe(SOLVER_REBALANCE_PLAN_FILES_URL)
    existing_jsons = [str(path).split("/")[-1] for path in SOLVER_PLAN_DATA_PATH.glob("*.json")]
    jsons_to_fetch = [json_path for json_path in df["Key"] if json_path not in existing_jsons]

    print(f"{len(jsons_to_fetch)=}", f"{len(existing_jsons)=}")

    for json_key in jsons_to_fetch:
        try:
            json_data = requests.get(
                f"https://ctrrwpvz5c.execute-api.us-east-1.amazonaws.com/GuardedLaunch/files/{json_key}"
            )

            with open(SOLVER_PLAN_DATA_PATH / json_key, "w") as fout:
                json.dump(json.loads(json_data.content), fout, indent=4)
                print("wrote", str(json_key))

        except Exception as e:
            print(e, type(e), json_key)


def _fetch_s3_contents_to_dataframe(url: str) -> pd.DataFrame:
    files = requests.get(url)
    tree = ET.ElementTree(ET.fromstring(files.content))
    root = tree.getroot()
    namespace = {"ns": "http://s3.amazonaws.com/doc/2006-03-01/"}
    contents_list = []
    for content in root.findall("ns:Contents", namespace):
        item = {
            "Key": content.find("ns:Key", namespace).text,
            "LastModified": content.find("ns:LastModified", namespace).text,
            "ETag": content.find("ns:ETag", namespace).text.replace('"', ""),
            "Size": int(content.find("ns:Size", namespace).text),
            "StorageClass": content.find("ns:StorageClass", namespace).text,
        }
        contents_list.append(item)
    return pd.DataFrame(contents_list)
