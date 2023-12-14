WITH pu AS (
    SELECT 
        CONCAT("F", YEAR(pickup_datetime), LPAD(MONTH(pickup_datetime), 2, '0'), LPAD(ROW_NUMBER() OVER (ORDER BY pickup_datetime, PUlocationID), 9, '0')) AS PickUpID,
        pickup_datetime as pudt, 
        PUlocationID as pul
    FROM fhv_record
    GROUP BY pickup_datetime, PUlocationID
),
do AS (
    SELECT
        CONCAT("F", YEAR(dropOff_datetime), LPAD(MONTH(dropOff_datetime), 2, '0'), LPAD(ROW_NUMBER() OVER (ORDER BY dropOff_datetime, DOlocationID), 9, '0')) AS DropOffID,
        dropOff_datetime, 
        DOlocationID
    FROM fhv_record
    GROUP BY dropOff_datetime, DOlocationID
)
-- info as(
SELECT 
    pu.PickUpID,
	do.DropOffID,
    bfr.dispatching_base_num, 
    bfr.Affiliated_base_number
FROM fhv_record AS bfr
LEFT JOIN pu ON pu.pudt = bfr.pickup_datetime AND pu.pul = bfr.PUlocationID
LEFT JOIN do ON do.dropOff_datetime = bfr.dropOff_datetime AND do.DOlocationID = bfr.DOlocationID
-- )
-- select count(*) from info



