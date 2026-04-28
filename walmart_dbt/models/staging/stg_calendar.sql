SELECT
    date                                        AS calendar_date,
    CAST(wm_yr_wk AS INT64)                     AS walmart_year_week,
    weekday                                     AS weekday_name,
    CAST(wday AS INT64)                         AS weekday_num,
    CAST(month AS INT64)                        AS month,
    CAST(year AS INT64)                         AS year,
    d                                           AS day_id,

    -- Event features
    COALESCE(event_name_1, 'No Event')          AS event_name_1,
    COALESCE(event_type_1, 'No Event')          AS event_type_1,
    COALESCE(event_name_2, 'No Event')          AS event_name_2,
    COALESCE(event_type_2, 'No Event')          AS event_type_2,

    -- Binary event flags (Major Deal Event)
    CASE WHEN event_name_1 IS NOT NULL
         OR event_name_2  IS NOT NULL
         THEN 1 ELSE 0 END                      AS is_event,
    
    CASE WHEN event_name_1 IS NOT NULL
         AND event_name_2 IS NOT NULL
         THEN 1 ELSE 0 END                      AS is_multiple_event,
    
    CASE WHEN event_name_1 = 'Thanksgiving'
         OR event_name_2  = 'Thanksgiving'
         THEN 1 ELSE 0 END                      AS is_Thanksgiving,
    
    CASE WHEN event_name_1 = 'MemorialDay'
         OR event_name_2  = 'MemorialDay'
         THEN 1 ELSE 0 END                      AS is_MemorialDay,

    CASE WHEN event_name_1 = 'LaborDay'
         OR event_name_2  = 'LaborDay'
         THEN 1 ELSE 0 END                      AS is_LaborDay,
    CASE WHEN event_name_1 = 'IndependenceDay'
         OR event_name_2  = 'IndependenceDay'
         THEN 1 ELSE 0 END                      AS is_IndependenceDay,
    CASE WHEN event_name_1 = 'Christmas'
         OR event_name_2  = 'Christmas'
         THEN 1 ELSE 0 END                      AS is_Christmas,
    CASE WHEN event_name_1 = 'NewYear'
         OR event_name_2  = 'NewYear'
         THEN 1 ELSE 0 END                      AS is_NewYear,

    -- Binary event flags (Holiday type)
    CASE WHEN event_type_1 = 'Sporting'
         OR event_type_2  = 'Sporting'
         THEN 1 ELSE 0 END                      AS is_sporting_event,

    CASE WHEN event_type_1 = 'National'
         OR event_type_2  = 'National'
         THEN 1 ELSE 0 END                      AS is_national_holiday,

    CASE WHEN event_type_1 = 'Religious'
         OR event_type_2  = 'Religious'
         THEN 1 ELSE 0 END                      AS is_religious_event,
    
    CASE WHEN event_type_1 = 'Cultural'
         OR event_type_2  = 'Cultural'
         THEN 1 ELSE 0 END                      AS is_cultural_event,

    -- SNAP benefit days per state
    CAST(COALESCE(snap_ca, 0) AS INT64)       AS snap_ca,
    CAST(COALESCE(snap_tx, 0) AS INT64)       AS snap_tx,
    CAST(COALESCE(snap_wi, 0) AS INT64)       AS snap_wi,

    -- Time features
    CASE WHEN CAST(wday AS INT64) IN (1, 2)
         THEN 1 ELSE 0 END                      AS is_weekend,

    CASE WHEN CAST(month AS INT64) IN (11, 12)
         THEN 1 ELSE 0 END                      AS is_holiday_season,

    CASE WHEN CAST(month AS INT64) IN (6, 7, 8)
         THEN 1 ELSE 0 END                      AS is_summer,
    
    CASE WHEN CAST(month AS INT64) IN (12, 1, 2)
         THEN 1 ELSE 0 END                      AS is_winter

FROM {{ source('walmart_raw', 'raw_calendar') }}
WHERE date IS NOT NULL