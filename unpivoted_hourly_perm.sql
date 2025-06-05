-- This unpivots the hourlydata so that traffice volume is the same column instead of separate columns. 
-- i.e. columns "hour_00, hour_01, hour_02 ..." -> column "volume"

WITH hourly_data AS (
    -- Unpivot hour_00 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '00' AS hour,
        hp.hour_00 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_01 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '01' AS hour,
        hp.hour_01 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_02 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '02' AS hour,
        hp.hour_02 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_03 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '03' AS hour,
        hp.hour_03 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_04 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '04' AS hour,
        hp.hour_04 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_05 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '05' AS hour,
        hp.hour_05 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_06 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '06' AS hour,
        hp.hour_06 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_07 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '07' AS hour,
        hp.hour_07 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_08 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '08' AS hour,
        hp.hour_08 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_09 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '09' AS hour,
        hp.hour_09 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_10 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '10' AS hour,
        hp.hour_10 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_11 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '11' AS hour,
        hp.hour_11 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_12 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '12' AS hour,
        hp.hour_12 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_13 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '13' AS hour,
        hp.hour_13 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_14 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '14' AS hour,
        hp.hour_14 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_15 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '15' AS hour,
        hp.hour_15 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_16 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '16' AS hour,
        hp.hour_16 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_17 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '17' AS hour,
        hp.hour_17 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_18 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '18' AS hour,
        hp.hour_18 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_19 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '19' AS hour,
        hp.hour_19 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_20 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '20' AS hour,
        hp.hour_20 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_21 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '21' AS hour,
        hp.hour_21 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_22 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb, -- Changed from sr.post_code to sr.suburb
        '22' AS hour,
        hp.hour_22 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'

    UNION ALL

    -- Unpivot hour_23 and join with station_reference
    SELECT
        hp.date,
        hp.public_holiday,
        hp.day_of_week,
        sr.name AS station_name,
        sr.suburb,
        '23' AS hour,
        hp.hour_23 AS volume
    FROM awsdatacatalog.newdatabase.hourly_permanent hp
    JOIN awsdatacatalog.newdatabase.station_reference sr
        ON hp.station_key = sr.station_key
    WHERE hp.year = '2025' AND hp.month = '01' AND hp.day BETWEEN '01' AND '31'
)
SELECT
    station_name,
    suburb,
    date,
    public_holiday,
    day_of_week,
    hour,
    SUM(volume) AS aggregated_volume -- Sum the volume for each unique combination
FROM hourly_data
WHERE
    volume IS NOT NULL 
    AND volume != 0 
GROUP BY
    station_name,
    suburb,
    date,
    public_holiday,
    day_of_week,
    hour -- Group by all non-aggregated columns
ORDER BY
    station_name,
    suburb,
    date,
    hour
LIMIT 100;
