# NYC-TripRecord (NewYorkCity-TripRecord)

## _New York City (NYC)_

![](images/taxi.png)

## I. Introduce

### 1. The goal of the project

Với chủ đề `Big Data With Spark`, nhóm mình mong muốn xây dựng một hệ thống (data pipeline) đơn giản, vận dụng kỹ thuật `ETL - (Extract - Transform - Load)` nhằm trích xuất, chuyển đổi và sử dụng những dữ liệu trên vào việc đánh giá và phát triển những dự án nhỏ trong tương lai.
Trong dự án này, nhóm mình sẽ minh họa rõ ràng và chi tiết các quy trình thực hiện `ETL` trên tập dữ liệu `NYC-TripRecord` - một tập dữ liệu mở, phục vụ cho việc học tập và nghiên cứu, Tại sao lại chọn dữ liệu này?

-   Nguồn dữ liệu `NYC-TripRecord` là tập dữ liệu tổng quan, chi tiết và khá phù hợp với dự án.
-   Giải thích cụ thể các bước`Extract - Transform - Load` lên trên hệ thống lưu trữ dữ liệu quan trọng.
-   Trình bày những khuynh hướng và quá trình xử lí những dữ liệu thô, sau đó mô phỏng và báo cáo sự đáng tin cậy của dữ liệu thông qua quá trình visualize và reporting.

### 2. Data Sources

`NYC-TripRecord` được chọn lọc từ tập dataset chính thức của `TLC-TripRecord`.
Dữ liệu bao gồm:

-   Yellow Taxi Trip Records: chứa các thuộc tính - pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.
-   Green Taxi Trip Records: chứa các thuộc tính - pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts, store and forward flags.
-   For_Hire Vehicle Trip Records (FHV Trip Records): chứa các thuộc tính - pick-up and drop-off dates/times, pick-up and drop-off locations, Dispatching-base-number (base license number) .

The dataset used

-   3 months (January - March / 2022) -
-   Taxi Zone Shapefile - https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip

> **TLC Trip Record Data:**
>
> Link website: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
>
> Data Dictionary:
> https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf > https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf > https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf

## II. Architecture

### 1. Overview

![](images/dicrectory_tree.png)
Chi tiết:

-   `app`: thư mục cấu hình và xây dựng visualize trên `streamlit`
-   `dagster_home`: Dagit and dagster daemno's configuarations
-   `dataset`: Nơi lưu trữ các file dữ liệu với `.format : .parquet` để đưa lên `MySQL`
-   `dockerimages`: bao gồm những docker-images tự thiết lập, ví dụ dagster, spark master, streamlit,...
-   `load_dataset`: bao gồm các file có dạng `.sql` để tạo các schema và đưa dữ liệu vào `SQL, Postgres`
-   `minio`:
-   `mysql`:
-   `postgresql`:
-   `Test`: Kiểm thử các dòng code đơn lẻ để đảm bảo sự chính xác.
-   `.gitugnore + .gitattributes`: Code versioning
-   `docker-compose`: để compose docker contatiners
-   `env`: biến môi trường (mặc định các thông số) - có thể thay đổi theo người sử dụng.
-   `Makefile`: đơn giản việc thực thi trên terminal's commands
-   `README.md`: Reportings Overall For The Project
-   `requirements.txt`: các thư viện cần thiết.

![](images/design_pipeline.png)

### 2. Containerize the application with `Docker` and orchestrate assets with `Dagster`.

### 3. Bronze layer

![](images/bronze.png)
Một vài xử lí nhỏ khi đọc file `taxizones.shp` để load các dữ liệu về kinh độ và vĩ độ

-   Sử dụng thư viện `GeoPandas` and `PyProj` và chuyển đổi từ `shapefile format` thành một `DataFrame`

        ```Python
        shapefile_path = f"{adr_data}/{zone_data}/{zone_data}.shp"

        gdf = gpd.read_file(shapefile_path)
        # Define
        source_crs = gdf.crs  # CRS of the shapefile
        target_crs = 'EPSG:4326'  # WGS84 - lat/lon CRS

        # Create a PyProj transformer
        transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

        gdf['longitude'] = gdf.geometry.centroid.x
        gdf['latitude'] = gdf.geometry.centroid.y
        gdf['longitude'], gdf['latitude'] = transformer.transform(gdf['longitude'], gdf['latitude'])

        df = pl.DataFrame(gdf[['LocationID', 'longitude', 'latitude']])
        ```

    Bao gồm các assets:

-   bronze_yellow_record: chứa data về yellow taxi NYC raw được lấy trực tiếp từ trang web
-   bronze_green_record: chứa data về green taxi NYC raw được lấy trực tiếp từ trang web
-   bronze_fhv_record: chứa data về fhv taxi NYC raw được lấy trực tiếp từ trang web
-   bronze_long_lat: chứa data về các kinh độ vĩ độ ở khu vực các taxi có thể đến

### 4. Silver layer

![](images/silver1.png)
![](images/silver2.png)
![](images/silver3.png)
Đây là bước xử lý data đã có được từ bronze layer.
Bao gồm các assets:

-   `Yellow`
    -   silver_yellow_pickup: PickUpID, Pickup_datetime, PULocationID
    -   silver_yellow_dropoff: DropOffID, Dropoff_datetime, DOLocationID
    -   silver_yellow_payment: PaymentID, Fare_amount, MTA_tax, Improvement_surcharge, Payment_type ,RateCodeID, Extra, Tip_amount, Tolls_amount, Total_amount, Congestion_Surcharge, Airport_fee
    -   silver_yellow_tripinfo: VendorID, PickUpID, DropOffID, PaymentID, Passenger_count, Trip_distance, Store_and_fwd_flag
-   `Green`
    -   silver_green_pickup: PickUpID, Pickup_datetime, PULocation
    -   silver_green_dropoff: DropOffID, Dropoff_datetime, DOLocation
    -   silver_green_payment: PaymentID, Fare_amount, MTA_tax, Improvement_surcharge, Payment_type, RateCodeID, Extra, Tip_amount, Tolls_amount, Total_amount
    -   silver_green_tripinfo: VendorID, PickUpID, DropOffID, PaymentID, Passenger_count,Trip_distance, Store_and_fwd_flag, Trip_type
-   `Fhv`
    -   silver_fhv_pickup: PickUpID, Pickup_datetime, PULocationID
    -   silver_fhv_dropoff: DropOffID, Dropoff_datetime, DOLocationID
    -   silver_fhvinfo: PickUpID, DropOffID, Dispatch_base_num, SR_Flag, Affiliated_base_number

### 5. Goal layer

![](images/gold.png)
Bao gồm các assets: - `gold_pickup` = silver_yellow_pickup + silver_green_pickup + silver_fhv_pickup + bronze_long_lat - `gold_dropoff` = silver_yellow_dropoff + silver_green_dropoff + silver_fhv_dropoff + bronze_long_lat - `gold_payment` = silver_yellow_payment + silver_green_payment - `gold_tripinfo` = silver_yellow_tripinfo + silver_green_tripinfo

### 6. Warehouse layer

![](images/warehouse.png)

    - warehouse_pickup = gold_pickup
    - warehouse_dropoff = gold_dropoff
    - warehouse_payment = gold_payment
    - warehouse_tripinfo = gold_tripinfo
    - warehouse_fhvinfo = silver_fhvinfo

-   Dữ liệu từ `gold_layer` và `silver_fhvinfo` sẽ được tải lên `Postgres` để lưu trữ và xử lí truong tương lai.

### 7. Visualize the data

-   Sử dụng `Streamlit Library` để visualize các dữ liệu
-   Dữ liệu được visualize được `connect` đến `Postgres` để lấy dữ liệu và chuyển đổi xử lí theo như mong muốn

## III. Data Lineage

![](images/general.png)

## IV. Result - Visualize

![](images/dashboard.png)
