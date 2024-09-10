import json
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from v2_rebalance_dashboard.constants import ROOT_DIR
from v2_rebalance_dashboard.get_rebalance_events_summary import fetch_clean_rebalance_events
import os


GET_REBALANCE_PLAN_FILE_NAMES_URL = "https://ctrrwpvz5c.execute-api.us-east-1.amazonaws.com/GuardedLaunch/files"
fetched_data_path = ROOT_DIR.parent / "fetched_data"

if not os.path.exists(fetched_data_path):
    os.mkdir(fetched_data_path)

autoETH = "0x49C4719EaCc746b87703F964F09C22751F397BA0"
balETH = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"


def fetch_s3_contents_to_dataframe(url):
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


def load_solver_df() -> pd.DataFrame:
    fetched_data_path = ROOT_DIR.parent / "fetched_data"
    existing_jsons = [str(path) for path in fetched_data_path.glob("*.json")]

    all_data = []
    for p in existing_jsons:
        try:
            with open(p, "r") as fin:
                json_file_file_name = p.split("/")[-1]
                solver_data = json.load(fin)
                solver_data["json_file_file_name"] = json_file_file_name
                solver_data["date"] = pd.to_datetime(solver_data["timestamp"], unit="s")

                if autoETH.lower() in json_file_file_name.lower():
                    solver_data["poolAddress"] = autoETH

                if balETH.lower() in json_file_file_name.lower():
                    solver_data["poolAddress"] = balETH

                all_data.append(solver_data)
        except Exception as e:
            pass
            # print(p, e)
    solver_df = pd.DataFrame.from_records(all_data)
    return solver_df


def load_balETH_solver_df():
    destination_df = pd.read_parquet(ROOT_DIR / "vaults.parquet")
    rebalance_event_df = fetch_clean_rebalance_events()

    destination_vault_to_name = {
        str(vault_address).lower(): name[22:]
        for vault_address, name in zip(destination_df["vaultAddress"], destination_df["name"])
    }
    destination_vault_to_name["0x72cf6d7c85ffd73f18a83989e7ba8c1c30211b73"] = "balETH idle"
    solver_df = load_solver_df()
    balETH_solver_df = solver_df[solver_df["poolAddress"] == balETH].copy()
    balETH_solver_df.set_index("date", inplace=True)

    balETH_solver_df["destinationInName"] = balETH_solver_df.apply(
        lambda row: (
            destination_vault_to_name[row["destinationIn"].lower()]
            if row["destinationIn"].lower() in destination_vault_to_name
            else None
        ),
        axis=1,
    )
    balETH_solver_df["destinationOutName"] = balETH_solver_df.apply(
        lambda row: (
            destination_vault_to_name[row["destinationOut"].lower()]
            if row["destinationOut"].lower() in destination_vault_to_name
            else None
        ),
        axis=1,
    )

    balETH_solver_df["moveName"] = balETH_solver_df.apply(
        lambda row: f"Exit {row['destinationOutName']} enter {row['destinationInName']}", axis=1
    )
    rebalance_event_df["moveName"] = rebalance_event_df.apply(
        lambda row: f"Exit {row['out_destination']} enter {row['in_destination']}", axis=1
    )
    balETH_proposed_rebalances_df = balETH_solver_df[balETH_solver_df["sodOnly"] == False].copy()
    return balETH_solver_df, balETH_proposed_rebalances_df, destination_df, rebalance_event_df


def make_proposed_vs_actual_rebalance_scatter_plot(
    balETH_solver_df: pd.DataFrame, rebalance_event_df: pd.DataFrame
) -> go.Figure:
    moves_df = pd.concat([balETH_solver_df["moveName"], rebalance_event_df["moveName"]], axis=1)
    moves_df.columns = ["proposed_rebalances", "actual_rebalances"]
    proposed_rebalances_fig = go.Scatter(
        x=moves_df.index,
        y=moves_df["proposed_rebalances"],
        mode="markers",
        name="Proposed Rebalances",
        marker=dict(color="blue", size=10),
    )

    # Create the plot with actual rebalances with red 'x' markers
    actual_rebalances_fig = go.Scatter(
        x=moves_df.index,
        y=moves_df["actual_rebalances"],
        mode="markers",
        name="Actual Rebalances",
        marker=dict(symbol="x", color="red", size=12),
    )

    # Combine both plots into one figure
    proposed_vs_actual_rebalance_scatter_plot_fig = go.Figure(data=[proposed_rebalances_fig, actual_rebalances_fig])

    # Update layout
    proposed_vs_actual_rebalance_scatter_plot_fig.update_layout(
        yaxis_title="Rebalances",
        xaxis_title="Date",
        title="balETH Proposed vs Actual Rebalances",
        height=600,
        width=600 * 2,
    )
    return proposed_vs_actual_rebalance_scatter_plot_fig


def get_proposed_vs_actual_rebalances(
    balETH_solver_df: pd.DataFrame, rebalance_event_df: pd.DataFrame, start_date: str = "8-01-2024"
):
    recent_solves = balETH_solver_df[balETH_solver_df.index > start_date]
    num_proposed_rebalances = (~recent_solves["sodOnly"].astype(bool)).sum()
    num_actual_rebalances = rebalance_event_df[rebalance_event_df.index > start_date].shape[0]
    rebalance_counts = {"Acutal": num_actual_rebalances, "Proposed": int(num_proposed_rebalances)}

    actual_vs_proposed_rebalance_bar_fig = go.Figure(
        data=[go.Bar(name="Rebalances", x=list(rebalance_counts.keys()), y=list(rebalance_counts.values()))]
    )

    # Update layout for better visualization
    actual_vs_proposed_rebalance_bar_fig.update_layout(
        title=f"Proposed vs Actual Rebalances after {start_date}",
        xaxis_title="Acutal Vs Proposed",
        yaxis_title="Count",
        bargap=0.2,
        bargroupgap=0.1,
        height=600,
        width=600,
    )
    return actual_vs_proposed_rebalance_bar_fig


def fetch_rebalance_plans_plots(autopool_name: str = "balETH") -> dict:
    if autopool_name != "balETH":
        raise ValueError("only works for balETH")
    _ensure_all_rebalance_plans_are_loaded()

    balETH_solver_df, balETH_proposed_rebalances_df, destination_df, rebalance_event_df = load_balETH_solver_df()
    proposed_vs_actual_rebalance_scatter_plot_fig = make_proposed_vs_actual_rebalance_scatter_plot(
        balETH_solver_df, rebalance_event_df
    )
    actual_vs_proposed_rebalance_bar_fig = get_proposed_vs_actual_rebalances(
        balETH_solver_df, rebalance_event_df, start_date="8-01-2024"
    )

    return {
        "proposed_vs_actual_rebalance_scatter_plot_fig": proposed_vs_actual_rebalance_scatter_plot_fig,
        "actual_vs_proposed_rebalance_bar_fig": actual_vs_proposed_rebalance_bar_fig,
    }
