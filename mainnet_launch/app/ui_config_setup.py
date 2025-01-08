"""Holds functions and config details that need to be run before starting the app"""

import streamlit as st
import plotly.io as pio
import plotly.express as px


def config_plotly_and_streamlit():
    # this needs to be first because otherwise we get this error:
    # `StreamlitAPIException: set_page_config() can only be called once per app page,
    # and must be called as the first Streamlit command in your script.`
    st.set_page_config(
        page_title="Mainnet Autopool Diagnostics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # set the default colors to be the 24 unique colors
    # see other colors here https://plotly.com/python/discrete-color/#color-sequences-in-plotly-express
    pio.templates["dark_24_color_template"] = pio.templates["plotly"]
    pio.templates["dark_24_color_template"]["layout"]["colorway"] = px.colors.qualitative.Dark24
    pio.templates.default = "dark_24_color_template"
