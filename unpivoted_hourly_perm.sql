CREATE TABLE processed_hourly_traffic
WITH (
    format = 'CSV',
    external_location = 's3://trafficsnswbucket/processed/traffic_volume/hourly_processed/',
    partitioned_by = ARRAY['year', 'month']
) AS
SELECT
    hp.station_key,
    hp.traffic_direction_seq,
    hp.cardinal_direction_seq,
    hp.classification_seq,
    hp.day_of_week,
    hp.public_holiday,
    hp.school_holiday,
    CAST(t.hour_of_day_str AS INT) AS hour_of_day,
    t.traffic_count,
    hp.year,
    hp.month
FROM
    hourly_permanent hp
CROSS JOIN UNNEST(
    -- Create a MAP where keys are hour strings and values are traffic counts
    MAP(
        ARRAY['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11',
              '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'],
        ARRAY[hp.hour_00, hp.hour_01, hp.hour_02, hp.hour_03, hp.hour_04, hp.hour_05,
              hp.hour_06, hp.hour_07, hp.hour_08, hp.hour_09, hp.hour_10, hp.hour_11,
              hp.hour_12, hp.hour_13, hp.hour_14, hp.hour_15, hp.hour_16, hp.hour_17,
              hp.hour_18, hp.hour_19, hp.hour_20, hp.hour_21, hp.hour_22, hp.hour_23]
    )
) AS t (hour_of_day_str, traffic_count) -- Unnesting a MAP produces two columns: key and value
WHERE t.traffic_count IS NOT NULL;
