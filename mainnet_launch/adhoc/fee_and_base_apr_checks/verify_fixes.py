"""
Verify that the VP fixes improved predicted vs actual fee+base APR alignment.
Loads the augmented plans from working_data/ and computes error metrics.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from mainnet_launch.constants import WORKING_DATA_DIR, AUTO_ETH, BASE_ETH, AutopoolConstants
from mainnet_launch.database.postgres_operations import get_full_table_as_df
from mainnet_launch.database.schema.full import Destinations


def _flatten_json(obj: Any, prefix: str = "", sep: str = ".") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}{sep}{k}" if prefix else str(k)
            out.update(_flatten_json(v, key, sep=sep))
        return out
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]"
            out.update(_flatten_json(v, key, sep=sep))
        return out
    out[prefix] = obj
    return out


def load_augmented_plans_as_df(
    autopool: AutopoolConstants,
    save_root: Path = WORKING_DATA_DIR,
) -> pd.DataFrame:
    plans_dir = save_root / f"{autopool.name}_augmented_plans"
    if not plans_dir.exists():
        raise FileNotFoundError(f"Augmented plans dir not found: {plans_dir}")

    rows: List[Dict[str, Any]] = []
    json_paths = sorted(plans_dir.glob("*.json"))

    for p in json_paths:
        with open(p, "r") as f:
            plan = json.load(f)
        for k in ["sod", "steps", "addRank", "rebalanceTest"]:
            plan.pop(k, None)
        flat = _flatten_json(plan, sep=".")
        flat["_file_path"] = str(p)
        flat["_file_name"] = p.name
        if "rebalance_plan_json_key" not in flat:
            flat["rebalance_plan_json_key"] = p.stem
        rows.append(flat)

    df = pd.DataFrame(rows)

    df["predicted_fee_and_base_out"] = (
        df["destinationOutSummaryStats.out.feeApr"] + df["destinationOutSummaryStats.out.baseApr"]
    ) * 100
    df["predicted_fee_and_base_in"] = (
        df["destinationOutSummaryStats.in.feeApr"] + df["destinationOutSummaryStats.in.baseApr"]
    ) * 100

    df["actual_fee_and_base_out"] = df["out_fee_and_base"]
    df["actual_fee_and_base_in"] = df["in_fee_and_base"]

    destinations_to_name = {
        d.destination_vault_address: d.name for d in get_full_table_as_df(Destinations).itertuples()
    }
    df["out_name"] = df["destinationOut"].map(destinations_to_name)
    df["in_name"] = df["destinationIn"].map(destinations_to_name)

    return df


def compute_metrics(df, pred_col, actual_col, label):
    """Compute error metrics between predicted and actual values."""
    d = df[[pred_col, actual_col]].dropna()
    d = d[np.isfinite(d[pred_col]) & np.isfinite(d[actual_col])]

    if len(d) == 0:
        print(f"  {label}: No valid data points")
        return

    diff = d[pred_col] - d[actual_col]
    abs_diff = diff.abs()

    print(f"\n  {label} ({len(d)} data points):")
    print(f"    MAE  = {abs_diff.mean():.4f}%")
    print(f"    RMSE = {np.sqrt((diff**2).mean()):.4f}%")
    print(f"    Median Abs Error = {abs_diff.median():.4f}%")
    print(f"    Max Abs Error    = {abs_diff.max():.4f}%")
    print(f"    Mean Bias (pred-actual) = {diff.mean():+.4f}%")

    # Count how many are within 1% and 2%
    within_1 = (abs_diff <= 1.0).sum()
    within_2 = (abs_diff <= 2.0).sum()
    print(f"    Within 1%: {within_1}/{len(d)} ({100*within_1/len(d):.1f}%)")
    print(f"    Within 2%: {within_2}/{len(d)} ({100*within_2/len(d):.1f}%)")


def check_specific_fixes(df, autopool_name):
    """Check if specific known-bad destinations are now fixed."""
    print(f"\n--- Specific Fix Verification for {autopool_name} ---")

    # Check osETH/rETH old DV (0x896e) - was showing actual ~0.07% with VP ~1.0025
    old_oseth = df[
        (df["destinationIn"] == "0x896eCc16Ab4AFfF6cE0765A5B924BaECd7Fa455a")
        | (df["destinationOut"] == "0x896eCc16Ab4AFfF6cE0765A5B924BaECd7Fa455a")
    ]
    if len(old_oseth) > 0:
        in_actuals = old_oseth["actual_fee_and_base_in"].dropna()
        out_actuals = old_oseth["actual_fee_and_base_out"].dropna()
        all_actuals = pd.concat([in_actuals, out_actuals])
        print(f"\n  osETH/rETH old DV (0x896e): {len(old_oseth)} events")
        print(f"    Before fix: actual ~0.07% (wrong VP ~1.0025)")
        if len(all_actuals) > 0:
            print(f"    After fix: actual mean={all_actuals.mean():.4f}%, median={all_actuals.median():.4f}%")
        # Check VP values
        for idx, row in old_oseth.head(3).iterrows():
            start_in = row.get("start_vp.in virtual_price", None)
            end_in = row.get("end_vp.in virtual_price", None)
            start_out = row.get("start_vp.out virtual_price", None)
            end_out = row.get("end_vp.out virtual_price", None)
            print(f"    VP sample: start_in={start_in}, end_in={end_in}, start_out={start_out}, end_out={end_out}")

    # Check Balancer ETHx/wstETH old DV (0xfB6f) - was showing actual ~0.03%
    old_ethx = df[
        (df["destinationIn"] == "0xfB6f99FdF12E37Bfe3c4Cf81067faB10c465fb24")
        | (df["destinationOut"] == "0xfB6f99FdF12E37Bfe3c4Cf81067faB10c465fb24")
    ]
    if len(old_ethx) > 0:
        in_actuals = old_ethx["actual_fee_and_base_in"].dropna()
        out_actuals = old_ethx["actual_fee_and_base_out"].dropna()
        all_actuals = pd.concat([in_actuals, out_actuals])
        print(f"\n  Balancer ETHx/wstETH old DV (0xfB6f): {len(old_ethx)} events")
        print(f"    Before fix: actual ~0.03% (wrong VP)")
        if len(all_actuals) > 0:
            print(f"    After fix: actual mean={all_actuals.mean():.4f}%, median={all_actuals.median():.4f}%")

    # Check wstETH-rETH-sfrxETH-BPT (0x5b39) - was showing -100% due to exploit-drained pool
    exploit_pool = df[
        (df["destinationIn"] == "0x5b39015f01A7b68093889678ee4e566959872A4A")
        | (df["destinationOut"] == "0x5b39015f01A7b68093889678ee4e566959872A4A")
    ]
    if len(exploit_pool) > 0:
        in_actuals = exploit_pool["actual_fee_and_base_in"]
        out_actuals = exploit_pool["actual_fee_and_base_out"]
        in_nulls = in_actuals.isna().sum()
        out_nulls = out_actuals.isna().sum()
        in_neg100 = (in_actuals == -100.0).sum()
        out_neg100 = (out_actuals == -100.0).sum()
        print(f"\n  wstETH-rETH-sfrxETH-BPT exploit pool (0x5b39): {len(exploit_pool)} events")
        print(f"    Before fix: actual was -100% (exploit-drained)")
        print(f"    After fix: in_nulls={in_nulls}, out_nulls={out_nulls}, in_neg100={in_neg100}, out_neg100={out_neg100}")

    # Check pxETH/stETH old DV (0xd96E) - was showing VP ~1.0025
    old_pxeth_steth = df[
        (df["destinationIn"] == "0xd96E943098B2AE81155e98D7DC8BeaB34C539f01")
        | (df["destinationOut"] == "0xd96E943098B2AE81155e98D7DC8BeaB34C539f01")
    ]
    if len(old_pxeth_steth) > 0:
        in_actuals = old_pxeth_steth["actual_fee_and_base_in"].dropna()
        out_actuals = old_pxeth_steth["actual_fee_and_base_out"].dropna()
        all_actuals = pd.concat([in_actuals, out_actuals])
        print(f"\n  pxETH/stETH old DV (0xd96E): {len(old_pxeth_steth)} events")
        print(f"    Before fix: actual ~0.3% (wrong VP ~1.0025)")
        if len(all_actuals) > 0:
            print(f"    After fix: actual mean={all_actuals.mean():.4f}%, median={all_actuals.median():.4f}%")

    # Check Curve ETHx-ETH old DV (0xC001) - was showing VP ~1.0025
    old_ethx_eth = df[
        (df["destinationIn"] == "0xC001f23397dB71B17602Ce7D90a983Edc38DB0d1")
        | (df["destinationOut"] == "0xC001f23397dB71B17602Ce7D90a983Edc38DB0d1")
    ]
    if len(old_ethx_eth) > 0:
        in_actuals = old_ethx_eth["actual_fee_and_base_in"].dropna()
        out_actuals = old_ethx_eth["actual_fee_and_base_out"].dropna()
        all_actuals = pd.concat([in_actuals, out_actuals])
        print(f"\n  Curve ETHx-ETH old DV (0xC001): {len(old_ethx_eth)} events")
        print(f"    Before fix: actual VP ~1.0025")
        if len(all_actuals) > 0:
            print(f"    After fix: actual mean={all_actuals.mean():.4f}%, median={all_actuals.median():.4f}%")

    # Check Aerodrome (0x945a) - was using same VP for start and end
    aero = df[
        (df["destinationIn"] == "0x945a4f719018edBa445ca67bDa43663C815835Ad")
        | (df["destinationOut"] == "0x945a4f719018edBa445ca67bDa43663C815835Ad")
    ]
    if len(aero) > 0:
        print(f"\n  Aerodrome weETH/WETH (0x945a): {len(aero)} events")
        print(f"    Before fix: same VP used for start and end (0% actual)")
        for idx, row in aero.head(3).iterrows():
            start_in = row.get("start_vp.in virtual_price", None)
            end_in = row.get("end_vp.in virtual_price", None)
            start_out = row.get("start_vp.out virtual_price", None)
            end_out = row.get("end_vp.out virtual_price", None)
            actual_in = row.get("actual_fee_and_base_in", None)
            actual_out = row.get("actual_fee_and_base_out", None)
            print(f"    VP: start_in={start_in}, end_in={end_in}, start_out={start_out}, end_out={end_out}")
            print(f"    Actual: in={actual_in}, out={actual_out}")

    # Check Gyroscope ECLP (0xBd13) - was returning ~0.003 with getRate()
    gyro = df[
        (df["destinationIn"] == "0xBd137c56f3116E5c36753037a784FF844F84F59c")
        | (df["destinationOut"] == "0xBd137c56f3116E5c36753037a784FF844F84F59c")
    ]
    if len(gyro) > 0:
        print(f"\n  Gyroscope ECLP cbETH/wstETH (0xBd13): {len(gyro)} events")
        print(f"    Before fix: getRate() returned ~0.003")
        for idx, row in gyro.head(3).iterrows():
            start_in = row.get("start_vp.in virtual_price", None)
            end_in = row.get("end_vp.in virtual_price", None)
            start_out = row.get("start_vp.out virtual_price", None)
            end_out = row.get("end_vp.out virtual_price", None)
            actual_in = row.get("actual_fee_and_base_in", None)
            actual_out = row.get("actual_fee_and_base_out", None)
            print(f"    VP: start_in={start_in}, end_in={end_in}, start_out={start_out}, end_out={end_out}")
            print(f"    Actual: in={actual_in}, out={actual_out}")


def show_top_errors(df, autopool_name, n=15):
    """Show top prediction errors."""
    print(f"\n--- Top {n} Prediction Errors for {autopool_name} ---")

    # OUT direction
    d = df[["out_name", "predicted_fee_and_base_out", "actual_fee_and_base_out"]].dropna()
    d = d[np.isfinite(d["predicted_fee_and_base_out"]) & np.isfinite(d["actual_fee_and_base_out"])]
    d = d[d["actual_fee_and_base_out"] >= -50]  # exclude extreme negatives
    d["diff"] = (d["predicted_fee_and_base_out"] - d["actual_fee_and_base_out"]).abs()
    top_out = d.nlargest(n, "diff")
    print(f"\n  OUT direction (top {n} by abs error):")
    for _, row in top_out.iterrows():
        print(f"    {row['out_name'][:50]:50s}  pred={row['predicted_fee_and_base_out']:7.3f}%  actual={row['actual_fee_and_base_out']:7.3f}%  diff={row['diff']:7.3f}%")

    # IN direction
    d = df[["in_name", "predicted_fee_and_base_in", "actual_fee_and_base_in"]].dropna()
    d = d[np.isfinite(d["predicted_fee_and_base_in"]) & np.isfinite(d["actual_fee_and_base_in"])]
    d = d[d["actual_fee_and_base_in"] >= -50]  # exclude extreme negatives
    d["diff"] = (d["predicted_fee_and_base_in"] - d["actual_fee_and_base_in"]).abs()
    top_in = d.nlargest(n, "diff")
    print(f"\n  IN direction (top {n} by abs error):")
    for _, row in top_in.iterrows():
        print(f"    {row['in_name'][:50]:50s}  pred={row['predicted_fee_and_base_in']:7.3f}%  actual={row['actual_fee_and_base_in']:7.3f}%  diff={row['diff']:7.3f}%")


def main():
    for autopool in [AUTO_ETH, BASE_ETH]:
        print(f"\n{'='*80}")
        print(f"  {autopool.name} - Augmented Plans Accuracy Report")
        print(f"{'='*80}")

        try:
            df = load_augmented_plans_as_df(autopool)
        except FileNotFoundError as e:
            print(f"  SKIPPED: {e}")
            continue

        print(f"\n  Total augmented plans loaded: {len(df)}")

        # Filter out rows where actual is None or -100 (exploit/bad data)
        df_clean = df.copy()
        # Keep all data for specific fix checks, but filter for metrics
        df_metrics = df_clean[
            (df_clean["actual_fee_and_base_out"].notna())
            & (df_clean["actual_fee_and_base_out"] > -50)
            & (df_clean["actual_fee_and_base_in"].notna())
            & (df_clean["actual_fee_and_base_in"] > -50)
        ]

        print(f"  After filtering extreme values: {len(df_metrics)} rows")

        # Overall metrics
        print(f"\n--- Overall Error Metrics for {autopool.name} ---")
        compute_metrics(df_metrics, "predicted_fee_and_base_out", "actual_fee_and_base_out", "OUT direction")
        compute_metrics(df_metrics, "predicted_fee_and_base_in", "actual_fee_and_base_in", "IN direction")

        # Specific fix verification
        check_specific_fixes(df, autopool.name)

        # Top errors
        show_top_errors(df, autopool.name)


if __name__ == "__main__":
    main()
