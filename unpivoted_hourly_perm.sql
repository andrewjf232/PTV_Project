-- This unpivots the hourlydata so that traffice volume is the same column instead of separate columns. 
-- i.e. columns "hour_00, hour_01, hour_02 ..." -> column "volume"

WITH hourly_data AS (
SELECT
    station_key, date, public_holiday, day_of_week,
    '00' AS hour,
    hour_00 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '01' AS hour,
    hour_01 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '02' AS hour,
    hour_02 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '03' AS hour,
    hour_03 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '04' AS hour,
    hour_04 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '05' AS hour,
    hour_05 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '06' AS hour,
    hour_06 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '07' AS hour,
    hour_07 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '08' AS hour,
    hour_08 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '09' AS hour,
    hour_09 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '10' AS hour,
    hour_10 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '11' AS hour,
    hour_11 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '12' AS hour,
    hour_12 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '13' AS hour,
    hour_13 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '14' AS hour,
    hour_14 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '15' AS hour,
    hour_15 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '16' AS hour,
    hour_16 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '17' AS hour,
    hour_17 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '18' AS hour,
    hour_18 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '19' AS hour,
    hour_19 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '20' AS hour,
    hour_20 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '21' AS hour,
    hour_21 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '22' AS hour,
    hour_22 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
UNION ALL

SELECT
    station_key, date, public_holiday, day_of_week,
    '23' AS hour,
    hour_23 AS volume
FROM awsdatacatalog.newdatabase.hourly_permanent
WHERE year = '2025' AND month = '01' AND day BETWEEN '01' AND '31'
)

SELECT * 
FROM hourly_data 
LIMIT 100
