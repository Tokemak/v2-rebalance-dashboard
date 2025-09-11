# not used, consider using later
from __future__ import annotations

from datetime import datetime, timezone
import os

from dotenv import load_dotenv
import requests

# not used, consider using later


load_dotenv()


def reset_dev_local_branch_to_match_main():
    project_id = os.getenv("NEON_PROJECT_ID")
    dev_branch_id = os.getenv("DEV_LOCAL_NEON_BRANCH_ID")
    production_branch_id = os.getenv("MAIN_NEON_BRANCH_ID")
    NEON_API_KEY = os.getenv("NEON_API_KEY")

    url = f"https://console.neon.tech/api/v2/projects/{project_id}/branches/{dev_branch_id}/restore"

    payload = {"source_branch_id": production_branch_id}

    headers = {"Authorization": f"Bearer {NEON_API_KEY}", "Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    if response.ok:
        print("Dev branch successfully reset to match production!")
    else:
        print("Error resetting dev branch:", response.status_code, response.text)


def reset_main_to_match_dev_local():
    project_id = os.getenv("NEON_PROJECT_ID")
    dev_branch_id = os.getenv("DEV_LOCAL_NEON_BRANCH_ID")
    production_branch_id = os.getenv("MAIN_NEON_BRANCH_ID")
    NEON_API_KEY = os.getenv("NEON_API_KEY")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    backup_name = f"main_backup_{timestamp}"

    url = f"https://console.neon.tech/api/v2/projects/{project_id}/branches/{production_branch_id}/restore"

    payload = {
        "source_branch_id": dev_branch_id,
        "preserve_under_name": backup_name,
    }
    headers = {"Authorization": f"Bearer {NEON_API_KEY}", "Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    if response.ok:
        print("main branch successfully reset to match dev!")
    else:
        print("Error resetting main branch:", response.status_code, response.text)


if __name__ == "__main__":
    # does not work
    reset_dev_local_branch_to_match_main()
    pass
