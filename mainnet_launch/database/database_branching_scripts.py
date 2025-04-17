import os

from dotenv import load_dotenv
import requests


load_dotenv()


def reset_dev_local_branch_to_match_production():
    """ """
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
