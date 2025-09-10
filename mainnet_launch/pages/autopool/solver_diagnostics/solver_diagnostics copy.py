# import json
# import pandas as pd
# import plotly.graph_objects as go
# import plotly.express as px
# from datetime import datetime, timedelta, timezone
# import streamlit as st
# from mainnet_launch.app.app_config import STREAMLIT_IN_MEMORY_CACHE_TIME, NUM_S3_BUCKET_FETCHING_THREADS

# import time
# import concurrent.futures
# import boto3
# from botocore import UNSIGNED
# from botocore.config import Config

# from mainnet_launch.constants import (
#     AutopoolConstants,
#     ALL_AUTOPOOLS,
#     SOLVER_REBALANCE_PLANS_DIR,
#     AUTO_ETH,
# )

# from mainnet_launch.destinations import get_destination_details
# from mainnet_launch.pages.rebalance_events.rebalance_events import (
#     fetch_rebalance_events_df,
# )

# from mainnet_launch.pages.solver_diagnostics.solver_profit import fetch_and_render_solver_profit_data
# from mainnet_launch.pages.solver_diagnostics.bps_lost_to_rebalances import fetch_and_render_bps_lost_to_rebalances


# def fetch_and_render_solver_diagnositics_data(autopool: AutopoolConstants):
#     fetch_and_render_solver_diagnostics_data(autopool)
#     fetch_and_render_solver_profit_data(autopool)
#     fetch_and_render_bps_lost_to_rebalances(autopool)


# def fetch_and_render_solver_diagnostics_data(autopool: AutopoolConstants):
#     ensure_all_rebalance_plans_are_loaded_from_s3_bucket()
#     solver_df = _load_solver_df(autopool)
#     if solver_df is None:
#         st.text(f"{autopool.name} has no rebalance plans on s3 bucket")
#         return

#     proposed_rebalances_df = solver_df[solver_df["sodOnly"] == False].copy()
#     proposed_rebalances_df.set_index("date", inplace=True)

#     rebalance_event_df = fetch_rebalance_events_df(autopool)
#     proposed_vs_actual_rebalance_scatter_plot_fig = _make_proposed_vs_actual_rebalance_scatter_plot(
#         proposed_rebalances_df, rebalance_event_df
#     )
#     bar_chart_count_proposed_vs_actual_rebalances_fig = _make_proposed_vs_actual_rebalances_bar_plot(
#         proposed_rebalances_df, rebalance_event_df
#     )

#     hours_between_plans_fig = _make_hours_between_fig(solver_df)

#     plan_count_fig = _make_count_of_solver_plans_each_day_plot(solver_df)

#     dex_win_fig = _dex_win_metrics(solver_df)

#     size_of_candidate_set = _add_add_rank_count(solver_df)

#     figs = [
#         proposed_vs_actual_rebalance_scatter_plot_fig,
#         bar_chart_count_proposed_vs_actual_rebalances_fig,
#         plan_count_fig,
#         hours_between_plans_fig,
#         dex_win_fig,
#         size_of_candidate_set,
#     ]
#     st.header(f"{autopool.name} Solver Diagnostics")
#     for fig in figs:
#         st.plotly_chart(fig, use_container_width=True)


# def ensure_all_rebalance_plans_are_loaded_from_s3_bucket():
#     s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
#     for autopool in ALL_AUTOPOOLS:
#         response = s3_client.list_objects_v2(Bucket=autopool.solver_rebalance_plans_bucket)
#         solver_plans_names_on_remote = response.get("Contents")
#         if solver_plans_names_on_remote is not None:
#             all_rebalance_plans = [o["Key"] for o in solver_plans_names_on_remote]
#             local_rebalance_plans = [str(path).split("/")[-1] for path in SOLVER_REBALANCE_PLANS_DIR.glob("*.json")]
#             rebalance_plans_to_fetch = [
#                 json_path for json_path in all_rebalance_plans if json_path not in local_rebalance_plans
#             ]
#             if len(rebalance_plans_to_fetch) > 0:

#                 def download_file(json_key):
#                     max_attempts = 3
#                     for attempt in range(max_attempts):
#                         try:
#                             s3_client.download_file(
#                                 autopool.solver_rebalance_plans_bucket,
#                                 json_key,
#                                 str(SOLVER_REBALANCE_PLANS_DIR / json_key),
#                             )
#                             return
#                         except Exception as e:
#                             if attempt == max_attempts - 1:
#                                 # give up and fetch it sequentally later
#                                 return
#                             else:
#                                 time.sleep((2**attempt) / 2)  # exponential backoff

#                 with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_S3_BUCKET_FETCHING_THREADS) as executor:
#                     executor.map(download_file, rebalance_plans_to_fetch)

#             # fetch any remaining that were not fetched seqentially
#             updated_local_rebalance_plans = [
#                 str(path).split("/")[-1] for path in SOLVER_REBALANCE_PLANS_DIR.glob("*.json")
#             ]
#             leftover_rebalance_plans_to_fetch = [
#                 json_path for json_path in all_rebalance_plans if json_path not in updated_local_rebalance_plans
#             ]
#             for json_key in leftover_rebalance_plans_to_fetch:
#                 s3_client.download_file(
#                     autopool.solver_rebalance_plans_bucket, json_key, SOLVER_REBALANCE_PLANS_DIR / json_key
#                 )


# # can be slow, requires loading a few thousand jsons.
# @st.cache_data(ttl=STREAMLIT_IN_MEMORY_CACHE_TIME)
# def _load_solver_df(autopool: AutopoolConstants) -> pd.DataFrame:
#     # not setup for if there are no rebalance plans
#     autopool_plans = [p for p in SOLVER_REBALANCE_PLANS_DIR.glob("*.json") if autopool.autopool_eth_addr in str(p)]

#     if len(autopool_plans) == 0:
#         return None

#     destination_details = get_destination_details(autopool)
#     destination_vault_address_to_symbol = {dest.vaultAddress: dest.vault_name for dest in destination_details}
#     all_data = []
#     for plan_json in autopool_plans:
#         with open(plan_json, "r") as fin:
#             data = json.load(fin)
#             data["date"] = pd.to_datetime(data["timestamp"], unit="s", utc=True)
#             if data["destinationIn"] in destination_vault_address_to_symbol:
#                 data["destinationIn"] = destination_vault_address_to_symbol[data["destinationIn"]]

#             if data["destinationOut"] in destination_vault_address_to_symbol:
#                 data["destinationOut"] = destination_vault_address_to_symbol[data["destinationOut"]]

#             data["moveName"] = f"{data['destinationOut']} -> {data['destinationIn']}"
#             all_data.append(data)
#     solver_df = pd.DataFrame.from_records(all_data)
#     solver_df.sort_values("date", ascending=True, inplace=True)
#     return solver_df


# def _make_proposed_vs_actual_rebalance_scatter_plot(
#     proposed_rebalances_df: pd.DataFrame, rebalance_event_df: pd.DataFrame
# ) -> go.Figure:
#     # TODO update this to use the real rebalance sizes.
#     proposed_rebalances_fig = go.Scatter(
#         x=proposed_rebalances_df.index,
#         y=proposed_rebalances_df["moveName"],
#         mode="markers",
#         name="Proposed Rebalances",
#         marker=dict(color="blue", size=10),
#         text=proposed_rebalances_df["amountOutETH"],
#         hovertemplate="Proposed ETH Amount Out: %{text}<extra></extra>",
#     )

#     actual_rebalances_fig = go.Scatter(
#         x=rebalance_event_df.index,
#         y=rebalance_event_df["moveName"],
#         mode="markers",
#         name="Actual Rebalances",
#         marker=dict(symbol="x", color="red", size=12),
#         text=rebalance_event_df["outEthValue"],
#         hovertemplate="Actual ETH Amount Out: %{text}<extra></extra>",
#     )

#     proposed_vs_actual_rebalance_scatter_plot_fig = go.Figure(data=[proposed_rebalances_fig, actual_rebalances_fig])

#     proposed_vs_actual_rebalance_scatter_plot_fig.update_layout(
#         yaxis_title="Rebalances",
#         xaxis_title="Date",
#         title="Proposed vs Actual Rebalances",
#         height=600,
#         width=600 * 3,
#     )
#     return proposed_vs_actual_rebalance_scatter_plot_fig


# def _make_proposed_vs_actual_rebalances_bar_plot(
#     proposed_rebalance_df: pd.DataFrame, rebalance_event_df: pd.DataFrame
# ) -> go.Figure:
#     today = datetime.now(timezone.utc)
#     seven_days_ago = today - timedelta(days=7)
#     thirty_days_ago = today - timedelta(days=30)
#     one_year_ago = today - timedelta(days=30)

#     records = []
#     for time_period, window in zip(
#         ["seven_days_ago", "thirty_days_ago", "one_year_ago"], [seven_days_ago, thirty_days_ago, one_year_ago]
#     ):
#         num_proposed_rebalances = sum(proposed_rebalance_df.index >= window)
#         num_actual_rebalances = sum(rebalance_event_df.index >= window)
#         records.append(
#             {
#                 "time_period": time_period,
#                 "num_actual_rebalances": num_actual_rebalances,
#                 "num_proposed_rebalances": num_proposed_rebalances,
#             }
#         )

#     proposed_and_actual_rebalance_counts_df = pd.DataFrame.from_records(records)
#     bar_chart_count_proposed_vs_actual_rebalances_fig = go.Figure()

#     bar_chart_count_proposed_vs_actual_rebalances_fig.add_trace(
#         go.Bar(
#             x=proposed_and_actual_rebalance_counts_df["time_period"],
#             y=proposed_and_actual_rebalance_counts_df["num_proposed_rebalances"],
#             name="Proposed Rebalances",
#             marker_color="blue",
#         )
#     )

#     bar_chart_count_proposed_vs_actual_rebalances_fig.add_trace(
#         go.Bar(
#             x=proposed_and_actual_rebalance_counts_df["time_period"],
#             y=proposed_and_actual_rebalance_counts_df["num_actual_rebalances"],
#             name="Actual Rebalances",
#             marker_color="green",
#         )
#     )

#     bar_chart_count_proposed_vs_actual_rebalances_fig.update_layout(
#         title="Proposed vs Actual Rebalances Over Time",
#         xaxis_title="Time Period",
#         yaxis_title="Count",
#         barmode="group",
#         bargap=0.15,
#         bargroupgap=0.1,
#         template="plotly",
#     )
#     return bar_chart_count_proposed_vs_actual_rebalances_fig


# def _make_count_of_solver_plans_each_day_plot(solver_df):
#     solver_count_per_day = solver_df.groupby(solver_df["date"].dt.date).size()
#     solver_count_per_day_df = pd.DataFrame(solver_count_per_day, columns=["Num Generated"])
#     return px.line(solver_count_per_day_df, title="Count of solver plans per day")


# def _dex_win_metrics(solver_df):
#     all_steps = solver_df["steps"].values
#     dex_steps = []

#     for steps in all_steps:
#         for step in steps:
#             if "dex" in step:
#                 dex_steps.append(step)

#     absolute_counts = pd.DataFrame.from_records(dex_steps)["dex"].value_counts()
#     normalized_counts = (100 * pd.DataFrame.from_records(dex_steps)["dex"].value_counts(normalize=True)).round(2)

#     # Combine the two DataFrames into one for display
#     combined_df = pd.DataFrame(
#         {
#             "DEX": absolute_counts.index,  # The index (dex names)
#             "Count of step": absolute_counts.values,  # The absolute counts
#             "Percent of steps": normalized_counts.values,  # The normalized counts
#         }
#     )

#     # Create the table figure
#     fig = go.Figure(
#         data=[
#             go.Table(
#                 header=dict(
#                     values=list(combined_df.columns), align="left"  # Use column names from the combined DataFrame
#                 ),  # Align header to the left
#                 cells=dict(
#                     values=[combined_df[col] for col in combined_df.columns],  # Table data from the DataFrame
#                     align="left",
#                 ),
#             )  # Align cells to the left
#         ]
#     )
#     fig.update_layout(title="Dex Aggregator Win Counts")

#     return fig


# def _make_hours_between_fig(solver_df):
#     solver_df["hoursBetween"] = (solver_df["date"].diff().dt.total_seconds()) / 3600
#     hours_between_plans_fig = px.scatter(
#         y=solver_df["hoursBetween"],
#         x=solver_df["date"],
#         title="Hours Between Rebalance Plans Generated",
#         labels={"x": "Rebalances", "y": "Hours"},
#     )
#     return hours_between_plans_fig


# def _add_add_rank_count(solver_df):
#     solver_df["len_addRank"] = solver_df["addRank"].apply(lambda x: len(x))
#     fig = px.line(solver_df, x="date", y="len_addRank", title="Candidate Destinations Size")
#     return fig


# if __name__ == "__main__":
#     # streamlit run mainnet_launch/pages/solver_diagnostics/solver_diagnostics.py
#     from mainnet_launch.constants import DINERO_ETH

#     fetch_and_render_solver_diagnositics_data(DINERO_ETH)
