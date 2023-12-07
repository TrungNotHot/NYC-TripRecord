create  table yellow_record
(
	 VendorID int,
	 tpep_pickup_datetime TIMESTAMP, 
	 tpep_dropoff_datetime TIMESTAMP,
     passenger_count float, 
     trip_distance float, 
     RatecodeID float, 
     store_and_fwd_flag varchar(32),
     PULocationID int, 
     DOLocationID int, 
     payment_type int, 
     fare_amount float, 
     extra float,
     mta_tax float, 
     tip_amount float, 
     tolls_amount float, 
     improvement_surcharge float,
     total_amount float,
     congestion_surcharge float,
     airport_fee float
);

create table green_record
(
	VendorID int, 
	lpep_pickup_datetime TIMESTAMP , 
	lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag varchar(50),
    RatecodeID float, 
    PULocationID int, 
    DOLocationID int,
    passenger_count float, 
    trip_distance float, 
    fare_amount float, 
    extra float, 
    mta_tax float,
    tip_amount float, 
    tolls_amount float, 
    ehail_fee varchar(50), 
    improvement_surcharge float,
    total_amount float, 
    payment_type float, 
    trip_type float, 
    congestion_surcharge float
);

create table fhv_record
(
	dispatching_base_num varchar(50), 
	pickup_datetime TIMESTAMP, 
	dropOff_datetime TIMESTAMP,
    PUlocationID float, 
    DOlocationID float,
    SR_Flag varchar(50), 
    Affiliated_base_number varchar(50)
)