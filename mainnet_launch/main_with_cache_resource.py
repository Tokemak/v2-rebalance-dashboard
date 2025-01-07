import streamlit as st
import os
import time
import pickle
from datetime import datetime


# Function to load data from disk
@st.cache_resource
def load_data_from_disk():
    print("this should only ever appear once")
    st.write("Initializing database... This will take a while.")
    print("sleeping for 5 seconds")
    time.sleep(5)  # Simulate a 10-minute operation

    data = {"key": "value"}  # Replace with actual data
    with open("database.pkl", "wb") as f:
        pickle.dump(data, f)
    st.write("Database initialized successfully.")
    with open("database.pkl", "rb") as f:
        data = pickle.load(f)
        return data


# Main app
def main():
    st.title("Streamlit App with Initialization Logic")

    if st.button("call a cache_resource function"):
        print("attempting to load data")
        data = load_data_from_disk()
        print("done loading data")
        st.write("Data loaded successfully:", data)


if __name__ == "__main__":
    main()
