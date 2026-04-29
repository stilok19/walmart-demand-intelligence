SELECT
    calendar_date,
    day_id,
    walmart_year_week,
    weekday_name,
    weekday_num,
    month,
    year,

    -- Event features
    event_name_1,
    event_type_1,
    event_name_2,
    event_type_2,
    is_event,
    is_multiple_event,

    -- Specific holidays
    is_Thanksgiving,
    is_Christmas,
    is_NewYear,
    is_MemorialDay,
    is_LaborDay,
    is_IndependenceDay,

    -- Event types
    is_sporting_event,
    is_national_holiday,
    is_religious_event,
    is_cultural_event,

    -- SNAP days
    snap_ca,
    snap_tx,
    snap_wi,

    -- Time features
    is_weekend,
    is_holiday_season,
    is_summer,
    is_winter,

    -- Days until next major holiday
    COUNT(CASE WHEN is_Thanksgiving = 1 THEN 1 END)
        OVER (
            ORDER BY calendar_date
            ROWS BETWEEN CURRENT ROW AND 30 FOLLOWING
        )                                       AS thanksgiving_in_30_days,

    COUNT(CASE WHEN is_Christmas = 1 THEN 1 END)
        OVER (
            ORDER BY calendar_date
            ROWS BETWEEN CURRENT ROW AND 30 FOLLOWING
        )                                       AS christmas_in_30_days,

    -- Week of month
    CEIL(EXTRACT(DAY FROM calendar_date) / 7)   AS week_of_month,

    -- Quarter
    EXTRACT(QUARTER FROM calendar_date)          AS quarter

FROM {{ ref('stg_calendar') }}
ORDER BY calendar_date