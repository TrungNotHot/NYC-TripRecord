import polars as pl
import psycopg2

def load_data_from_postgres(start_date, end_date, drop_or_pick):
    try:
        # Connection parameters
        host = 'localhost'
        port = 5432
        database = 'trip_record'
        user = 'admin'
        password = 'admin123'

        # Replace 'warehouse' with the actual schema name
        schema_name = 'warehouse'

        # Connection URL
        connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        # Establish a connection
        conn = psycopg2.connect(connection_url)

        cursor = conn.cursor()

        cursor.execute(f"SET search_path TO {schema_name}")

        if drop_or_pick == "Pick Up":
            sql_query = f"""
                SELECT 
                    wp.pickup_datetime,
                    wp."PULocationID",
                    wp.longitude,
                    wp.latitude,
                    wp2.mta_tax,
                    wp2.fare_amount,
                    wp2.payment_type,
                    wp2.tip_amount,
                    wp2.total_amount,
                    wi.passenger_count,
                    wi.trip_distance
                FROM 
                    warehouse.warehouse_info AS wi
                LEFT JOIN 
                    warehouse.warehouse_pickup AS wp ON wp."PickUpID" = wi."PickUpID" 
                LEFT JOIN 
                    warehouse.warehouse_payment AS wp2 ON wi."PaymentID" = wp2."PaymentID" 
                WHERE 
                    DATE(wp.pickup_datetime) >= '{start_date}' 
                    AND DATE(wp.pickup_datetime) <= '{end_date}';
                """
        else:
            sql_query = f"""
                SELECT 
                    wd."Dropoff_datetime",
                    wd."LocationID",
                    wd.longitude,
                    wd.latitude,
                    wp2.mta_tax,
                    wp2.fare_amount,
                    wp2.payment_type,
                    wp2.tip_amount,
                    wp2.total_amount,
                    wi.passenger_count,
                    wi.trip_distance
                FROM 
                    warehouse.warehouse_info AS wi
                LEFT JOIN 
                    warehouse.warehouse_dropoff AS wd ON wi."DropOffID" = wd."DropOffID" 
                LEFT JOIN 
                    warehouse.warehouse_payment AS wp2 ON wi."PaymentID" = wp2."PaymentID" 
                WHERE 
                    DATE(wd."Dropoff_datetime") >= '{start_date}' 
                    AND DATE(wd."Dropoff_datetime") <= '{end_date}';
            """
        # Read data from the database
        df = pl.read_database(sql_query, conn)

        # Close the cursor and connection
        conn.commit()

        cursor.close()
        conn.close()

        return df
    except Exception as e:
        print(f"Error occurred while loading data from PostgreSQL: {e}")
        raise e
