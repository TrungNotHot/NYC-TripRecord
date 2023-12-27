import geopandas as gpd
import pyproj
import polar as pl
import pandas as pd
import folium
from folium.plugins import HeatMap

shapefile_path = "app/map/taxi_zones.shp"

gdf = gpd.read_file(shapefile_path)
# Define
source_crs = gdf.crs  # CRS of the shapefile
target_crs = 'EPSG:4326'  # WGS84 - lat/lon CRS
transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

gdf['longitude'] = gdf.geometry.centroid.x
gdf['latitude'] = gdf.geometry.centroid.y
gdf['longitude'], gdf['latitude'] = transformer.transform(gdf['longitude'], gdf['latitude'])

df = pd.DataFrame(gdf[['LocationID', 'longitude', 'latitude']])
df.to_parquet("app/map/taxi_zones.parquet")
# print(df)


# Khởi tạo một bản đồ Folium
m = folium.Map(location=[40.7128, -74.0060], zoom_start=12,  tiles='CartoDB dark_matter')  # Vị trí trung tâm và mức độ zoom

# Tạo dữ liệu cho heatmap từ DataFrame df
data_heatmap = df[['latitude', 'longitude']].values.tolist()

# # Tạo layer heatmap
# heat_map = HeatMap(data_heatmap)

# Thêm layer heatmap vào bản đồ
# heat_map.add_to(m)

# Tính toán bán kính dựa trên mức độ tập trung của vị trí
min_concentration = df['LocationID'].min()
max_concentration = df['LocationID'].max()

# Min-max scaling để chuyển đổi mức độ tập trung thành bán kính
def calculate_radius(concentration):
    return (concentration - min_concentration) / (max_concentration - min_concentration) * 30  # Scale radius between 0 and 10

# Thêm các CircleMarker với bán kính phụ thuộc vào mức độ tập trung
for i in range(len(df)):
    concentration = df.iloc[i]['LocationID']
    radius = calculate_radius(concentration)
    folium.CircleMarker(
        radius=radius,
        color='red',
        fill=True,
        fill_color='red',
        location=[df.iloc[i]['latitude'], df.iloc[i]['longitude']],
        popup=f"Location ID: {df.iloc[i]['LocationID']}"
    ).add_to(m)

# # Tạo chú thích cho mức độ màu của heatmap
# gradient = {0.4: 'blue', 0.6: 'lime', 1: 'red'}
# heatmap_with_gradient = HeatMap(data_heatmap, gradient=gradient)

# # Thêm chú thích heatmap vào LayerControl
# folium.LayerControl(collapsed=False).add_to(m)


# Hiển thị bản đồ
m.save("app/map/my_heatmap.html")
