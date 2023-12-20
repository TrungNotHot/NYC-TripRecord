--CREATE DATABASE IF NOT EXISTS trip_record;

--CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS public.warehouse_pickup(
   PickUpID INT NOT NULL PRIMARY KEY, 
   pickup_datetime TIMESTAMP, 
   PULocationID INT,
   latitude DECIMAL(13,10) NOT NULL,
   longitude DECIMAL(13,10) NOT NULL
);

-- DropOffID, Dropoff_datetime, DOLocationID
CREATE TABLE IF NOT EXISTS public.warehouse_dropoff(
   DropOffID INT NOT NULL PRIMARY KEY, 
   dropoff_datetime TIMESTAMP, 
   DOLocationID INT,
   latitude DECIMAL(13,10) NOT NULL,
   longitude DECIMAL(13,10) NOT NULL
);

-- PaymentID, Fare_amount, MTA_tax, Improvement_surcharge, Payment_type,
-- RateCodeID, Extra, Tip_amount, Tolls_amount, Total_amount, Congestion_Surcharge, Airport_fee
CREATE TABLE IF NOT EXISTS public.warehouse_payment(
   PaymentID INT NOT NULL PRIMARY KEY, 
   fare_amount FLOAT,
   mta_tax FLOAT,
   improvement_surcharge FLOAT,
   payment_type FLOAT,
   RatecodeID INT,
   extra FLOAT,
   tip_amount FLOAT,
   tolls_amount FLOAT,
   total_amount FLOAT,
   congestion_surcharge FLOAT,
   airport_fee FLOAT
);

-- VendorID, PickUpID, DropOffID, PaymentID, Passenger_count,Trip_distance, Store_and_fwd_flag,  Trip_type
CREATE TABLE IF NOT EXISTS public.warehouse_info(
   VendorID INT NOT NULL PRIMARY KEY, 
   PickUpID INT, 
   DropOffID INT, 
   PaymentID INT, 
   passenger_count INT,
   trip_distance FLOAT, 
   store_and_fwd_flag VARCHAR(5), 
   trip_type FLOAT
);

-- PickUpID, DropOffID, Dispatch_base_num, SR_Flag, Affiliated_base_number
CREATE TABLE IF NOT EXISTS public.warehouse_fhv_info(
   PickUpID INT, 
   DropOffID INT, 
   dispatch_base_num VARCHAR(10), 
   sr_flag VARCHAR(10), 
   affiliated_base_number VARCHAR(10)
);
