import subprocess
import sys


def app():
    # forward any extra args on the end if you like
    subprocess.run(["streamlit", "run", "mainnet_launch/app/main.py", *sys.argv[1:]], check=True)
