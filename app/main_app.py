import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import datetime
import polars as pl

from streamlit_option_menu import option_menu
from query import *
from dataframe_function import  main
from connect_postgres import load_data_from_postgres



st.set_page_config(page_title="Dashboard", page_icon="🏎", layout="wide")
st.subheader("⛩ 🌐 Trip Data Analysis")
st.markdown("##")

with st.sidebar:
    # side bar
    st.sidebar.image("app/taxi.png", caption = "Category Taxi-Trip Analysis", )

    bucket = "None"
    name_your_data = "None"
    bucket = st.sidebar.selectbox("Choose Dataset on WareHouse ... " , {"None", "WareHouse"})
    if bucket != "None":
        st.sidebar.header("Please filter...")

        name_your_data = st.sidebar.selectbox("Choose Dataset on Your Dataset ... " , {"PickUp", "DropOff"})
        name_your_data = name_your_data.lower()

        today = datetime.datetime.now()
        year_current = today.year
        jan_1 = datetime.date(year_current, 1, 1)
        feb_2 = datetime.date(year_current, 2, 28)
        date_from = st.date_input("From - To",(jan_1, datetime.date(year_current, 1, 10)),jan_1, feb_2,format="MM.DD.YYYY")

    else:
        st.sidebar.markdown("##")

    bucket = bucket.lower()
    table_name = bucket + "_" + name_your_data

def main1(df, name_data):
    if bucket != "none":
        if len(date_from) > 1:
            df = df.filter(df["longitude"].is_not_null() & df["latitude"].is_not_null())
        if not df.is_empty():  
            main(df, name_data)
        else:
            st.title("Không có nguồn dữ liệu để phân tích....")

    else:
        name_df = "Không có nguồn dữ liệu để phân tích...."
        st.title(f"{name_df}")

# print(pd.DataFrame(df))
if __name__ == "__main__":
    if name_your_data =="pickup":
        df = load_data_from_postgres(date_from[0],date_from[1], "Pick Up")
    else:
        df = load_data_from_postgres(date_from[0],date_from[1], "Drop Off")
    main1(df, name_your_data)
    hide_st_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
"""





