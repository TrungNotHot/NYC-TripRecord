DROP DATABASE IF EXISTS trip_record;
CREATE DATABASE trip_record;
USE trip_record;

-- load yellow_record dataset
DROP TABLE IF EXISTS trip_record.yellow_record;
CREATE TABLE trip_record.yellow_record
(
	 VendorID INT,
	 tpep_pickup_datetime TIMESTAMP, 
	 tpep_dropoff_datetime TIMESTAMP,
     passenger_count FLOAT, 
     trip_distance FLOAT, 
     RatecodeID INT, 
     store_and_fwd_flag VARCHAR(32),
     PULocationID INT, 
     DOLocationID INT, 
     payment_type INT, 
     fare_amount FLOAT, 
     extra FLOAT,
     mta_tax FLOAT, 
     tip_amount FLOAT, 
     tolls_amount FLOAT, 
     improvement_surcharge FLOAT,
     total_amount FLOAT,
     congestion_surcharge FLOAT,
     airport_fee FLOAT
);

-- load green_record dataset
DROP TABLE IF EXISTS trip_record.green_record;
CREATE TABLE trip_record.green_record
(
	VendorID INT, 
	lpep_pickup_datetime TIMESTAMP , 
	lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag VARCHAR(50),
    RatecodeID INT, 
    PULocationID INT, 
    DOLocationID INT,
    passenger_count FLOAT, 
    trip_distance FLOAT, 
    fare_amount FLOAT, 
    extra FLOAT, 
    mta_tax FLOAT,
    tip_amount FLOAT, 
    tolls_amount FLOAT, 
    ehail_fee VARCHAR(50), 
    improvement_surcharge FLOAT,
    total_amount FLOAT, 
    payment_type FLOAT, 
    trip_type FLOAT, 
    congestion_surcharge FLOAT
);

-- load fvh_record dataset
DROP TABLE IF EXISTS trip_record.fhv_record;
CREATE TABLE trip_record.fhv_record
(
	dispatching_base_num VARCHAR(50), 
	pickup_datetime TIMESTAMP, 
	dropOff_datetime TIMESTAMP,
    PUlocationID INT, 
    DOlocationID INT,
    SR_Flag VARCHAR(50), 
    Affiliated_base_number VARCHAR(50)
);

-- load fvh_record dataset
DROP TABLE IF EXISTS long_lat;
CREATE TABLE long_lat
(
	LocationID INT, 
    longitude DECIMAL(13,10), 
    latitude DECIMAL(13,10)
);



