import subprocess
import sys

# needed for poetry scripts


def app():
    subprocess.run(["streamlit", "run", "mainnet_launch/app/main.py", *sys.argv[1:]], check=True)


def marketing_app():
    subprocess.run(
        ["streamlit", "run", "mainnet_launch/app/marketing_app/marketing_main.py", *sys.argv[1:]], check=True
    )
