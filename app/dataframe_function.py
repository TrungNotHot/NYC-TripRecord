import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import os
import folium
import polars as pl

## Cài thêm streamlit_extras
from streamlit_extras.metric_cards import style_metric_cards
from query import *

def calculate_radius(concentration, min_concentration, max_concentration):
    max_min = max_concentration - min_concentration
    if max_min == 0:
         return 0
    return (concentration - min_concentration) / (max_concentration - min_concentration) * 30  # Scale radius between 0 and 10

def main(df,name_data):
    title_name = name_data.upper()
    st.title(f"{name_data} INFORMATION")
    st.dataframe(df[df.columns[0:]], use_container_width=True)
    st.write(f"""<p style="text-align:center;font-szie:24px; color: black;font-weight: 600;> "Dataset of the PickUP Information" </p>""", unsafe_allow_html=True)
    max_trip_distance = float(df["trip_distance"].max())
    max_trip_distance = round(max_trip_distance, 3)

    min_trip_distance = float(df["trip_distance"].min())
    min_trip_distance = round(min_trip_distance, 3)

    avg_trip_distance = float(df["trip_distance"].mean())
    avg_trip_distance = round(avg_trip_distance, 3)

    max_passenger_count = float(df["passenger_count"].max())
    max_passenger_count = round(max_passenger_count, 3)

    min_passenger_count = float(df["passenger_count"].min())
    min_passenger_count = round(min_passenger_count, 3)

    avg_passenger_count = float(df["passenger_count"].mean())
    avg_passenger_count = round(avg_passenger_count, 3)
    coll1, coll2,coll3 = st.columns(3)

    style = """
        border: 2px solid #ff0090;
        border-radius: 12% 186px 0%;
        padding: 20px;
        margin-left: 4rem;
        margin-right: 4rem;
        box-shadow: rgb(52 217 168 / 84%) 0px 0.25rem 0.75rem;
        text-align: center;
        font-weight: 600;
    """

    # Hiển thị các thẻ metric trong các cột với kiểu được thiết lập
    with coll1:
        st.markdown(
            f"""
            <div style="{style}">
                <p style= "color: white ;font-weight: 600; font-size : 18px">Max Trip Distance</p>
                <p style= "color: cyan ;font-weight: 600; font-size : 30px">{max_trip_distance}</p>
            </div>
            <div style="{style} ; margin-top : 2rem; margin-bottom : 2rem">
                <p style= "color: white ;font-weight: 600; font-size : 18px">Max Passenger Count</p>
                <p style= "color: cyan ;font-weight: 600; font-size : 30px">{max_passenger_count}</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    with coll2:
        st.markdown(
            f"""
            <div style="{style} ">
                <p style= "color: white ;font-weight: 600; font-size : 18px">Min Trip Distance</p>
                <p style= "color: cyan ;font-weight: 600; font-size : 30px">{min_trip_distance}</p>
            </div>
            <div style="{style} ; margin-top : 2rem; margin-bottom : 2rem">
                <p style= "color: white ;font-weight: 600; font-size : 18px">Min Passenger Count</p>
                <p style= "color: cyan ;font-weight: 600; font-size : 30px">{min_passenger_count}</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    with coll3:
        st.markdown(
            f"""
            <div style="{style}">
                <p style= "color: white ;font-weight: 600; font-size : 18px">Avg Trip Distance</p>
                <p style= "color: cyan ;font-weight: 600; font-size : 30px">{avg_trip_distance}</p>
            </div>
            <div style="{style} ; margin-top : 2rem; margin-bottom : 2rem">
                <p style= "color: white ;font-weight: 600; font-size : 18px">Avg Passenger Count</p>
                <p style= "color: cyan ;font-weight: 600; font-size : 30px">{avg_passenger_count}</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    col1, col2= st.columns((1, 2))
    with col1: 
                st.image("app/taxi_yellow.png", caption="Taxi Trip Yellow", use_column_width=True,clamp=True)
                avg_tip_amount = float(df["tip_amount"].mean())
                avg_tip_amount = round(avg_tip_amount, 3)
                # print(avg_tip_amount)
                avg_fare_amount = float(df["fare_amount"].mean())
                avg_fare_amount = round(avg_fare_amount, 3)

                avg_total_amount = float(df["total_amount"].mean())
                avg_total_amount = round(avg_total_amount, 3)
                st.markdown(f"""<div style = "display : wrap "> 
                            <div style =' padding-top: 10px ; margin: 1rem 6rem; border: 2px solid cyan; border-bottom-right-radius: 50%;border-bottom-left-radius: 50%;'>
                                <p style="text-align:center;font-szie:24px; color: white ;font-weight: 600;">Avg Tip Amount</p>
                                <p style="text-align:center; color: red; font-size: 30px;font-weight: 600;" >{avg_tip_amount}</p>
                            </div>
                            <div  style ='padding-top: 10px; margin: 1rem 6rem; border: 2px solid cyan;  border-bottom-right-radius: 50%;border-bottom-left-radius: 50%;margin-top: 2rem;padding: 0 10px;'> 
                                <p style="text-align:center;font-szie:24px; color: white ;font-weight: 600;">Avg Fare Amount</p>
                                <p style="text-align:center; color: red; font-size: 30px;font-weight: 600;" >{avg_fare_amount}</p>
                            </div>
                            <div  style =' padding-top: 10px;margin: 1rem 6rem;border: 2px solid cyan; border-bottom-right-radius: 50%;border-bottom-left-radius: 50%;margin-top: 2rem;padding: 0 10px;'> 
                                <p style="text-align:center;font-szie:24px; color: white ;font-weight: 600;">Avg Total Amount</p>
                                <p style="text-align:center; color: red; font-size: 30px;font-weight: 600;" >{avg_total_amount}</p>
                            </div>
                            </div>"""
                            , unsafe_allow_html=True)  

    with col2:
                # Group by "longitude" and "latitude" columns and select the first row of each group
        df = df.groupby(["longitude", "latitude"]).agg(pl.first("*"))
        # print(df)
        # Khởi tạo một bản đồ FoliumSS
        m = folium.Map(location=[40.7128, -74.0060], zoom_start=12,  tiles='CartoDB dark_matter')  # Vị trí trung tâm và mức độ zoom
        if name_data == "pickup":
            temp = df['PULocationID']
        else:
            temp = df['DOLocationID']
        # Tính toán bán kính dựa trên mức độ tập trung của vị trí
        min_concentration = temp.min()
        max_concentration = temp.max()

        # Thêm các CircleMarker với bán kính phụ thuộc vào mức độ tập trung
        for i in range(len(df)):
            # print(i)
            concentration = temp[i]
            radius = calculate_radius(concentration, min_concentration, max_concentration)
            folium.CircleMarker(
                radius=radius,
                color='red',
                fill=True,
                fill_color='red',
                location=[df['latitude'][i], df['longitude'][i]],
                popup=f"Location ID: {temp[i]}"
            ).add_to(m)



        # Hiển thị bản đồ
        m.save(f"app/map/my_heatmap_pickup.html")
        path_file_html = f"app/map/my_heatmap_pickup.html"
        if os.path.exists(path_file_html):    
            with open(path_file_html, "r") as file:
                map_html = file.read()

            st.components.v1.html(map_html, height=700)
    col1, col2 = st.columns(2)
    with col1:
        # Vẽ biểu đồ line cho 2 thuộc tính mta_amount và fare_amount
        fig = px.scatter(df.to_pandas(), x=df['tip_amount'], y=df['fare_amount'], title="Biểu đồ biểu diễn sự phụ thuộc giữa 2 thuộc tính Số tiền tip và Lượng Phí đi.")
        # Đặt tên cho trục x và trục y
        fig.update_xaxes(title_text='Tip Amount')
        fig.update_yaxes(title_text='Fare Amount')
        st.plotly_chart(fig)
    with col2:

        # Vẽ biểu đồ line cho 2 thuộc tính mta_amount và fare_amount
        fig = px.bar(df.to_pandas(), y=df['mta_tax'], x=df['fare_amount'], title="Biểu đồ biểu diễn sự phụ thuộc giữa 2 thuộc tính Thuế và Lượng Phí đi.")
        # Đặt tên cho trục x và trục y
        fig.update_yaxes(title_text='Mta Tax')
        fig.update_xaxes(title_text='Fare Amount')
        st.plotly_chart(fig)

    # Vẽ biểu đồ line cho 2 thuộc tính mta_amount và fare_amount
    colss = ["tip_amount", "fare_amount", "mta_tax", "total_amount", "passenger_count", "trip_distance"]


    fig = px.line(df[colss].to_pandas(), title = "Mối quan hệ giữa các chi phí")
    fig.update_yaxes(title_text='$')
    fig.update_xaxes(title_text='$')
    fig.update_layout(title={'text': 'Mối quan hệ giữa các chi phí', 'x': 0.45, 'y': 0.95, 'xanchor': 'center', 'yanchor': 'top'})
    st.plotly_chart(fig, use_container_width=True)
       