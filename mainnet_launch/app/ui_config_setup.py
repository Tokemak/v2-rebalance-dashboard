"""Holds functions and config details that need to be run before starting the app"""

import streamlit as st
import plotly.io as pio
import plotly.express as px


STREAMLIT_MARKDOWN_HTML = """
        <style>
        .main {
            max-width: 85%;
            margin: 0 auto;
            padding-top: 40px;
        }
        .stPlotlyChart {
            width: 100%;
            height: auto;
            min-height: 300px;
            max-height: 600px;
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 20px;
        }
        @media (max-width: 768px) {
            .stPlotlyChart {
                min-height: 250px;
                max-height: 450px;
            }
        }
        .stPlotlyChart {
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 10px;
        }
        .stExpander {
            background-color: #e6e9ef;
            border-radius: 5px;
            padding: 10px;
        }
        </style>
        """


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


def format_timedelta(td):
    """Format a timedelta object into a readable string."""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = int(td.microseconds / 1000)
    formatted_time = ""
    if hours > 0:
        formatted_time += f"{hours}h "
    if minutes > 0 or hours > 0:
        formatted_time += f"{minutes}m "
    formatted_time += f"{seconds}s {milliseconds}ms"
    return formatted_time
