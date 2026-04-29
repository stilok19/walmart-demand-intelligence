SELECT
    -- Identity
    s.item_id,
    s.store_id,
    #s.state_id,
    s.dept_id,
    s.day_id,
    s.day_num,
    s.category,
    s.department,
    #s.item,
    s.state,
    s.store_number,

    -- Target variable
    s.sales,
    s.is_no_sales,

    -- Lag features
    s.lag_1,
    s.lag_7,
    s.lag_14,
    s.lag_28,
    s.lag_35,

    -- Rolling features
    s.rolling_mean_7,
    s.rolling_mean_28,
    s.rolling_mean_56,
    s.rolling_std_7,
    s.rolling_std_28,
    s.rolling_max_28,
    s.no_sales_last_7_days,
    s.cumulative_sales,

    -- Calendar features
    c.calendar_date,
    c.weekday_name,
    c.weekday_num,
    c.month,
    c.year,
    c.quarter,
    c.week_of_month,
    c.is_weekend,
    c.is_holiday_season,
    c.is_summer,
    c.is_winter,
    c.is_event,
    c.is_multiple_event,
    c.is_Thanksgiving,
    c.is_Christmas,
    c.is_NewYear,
    c.is_MemorialDay,
    c.is_LaborDay,
    c.is_IndependenceDay,
    c.is_sporting_event,
    c.is_national_holiday,
    c.is_religious_event,
    c.is_cultural_event,
    c.thanksgiving_in_30_days,
    c.christmas_in_30_days,

    -- SNAP features
    CASE
        WHEN s.state = 'CA' THEN c.snap_ca
        WHEN s.state = 'TX' THEN c.snap_tx
        WHEN s.state = 'WI' THEN c.snap_wi
        ELSE 0
    END                                         AS is_snap_day,

    -- Price features
    p.sell_price,
    p.price_change,
    p.price_change_pct,
    p.avg_price_rolling_4wk,
    p.price_momentum_4wk,
    p.is_on_promotion,
    p.relative_price_in_category,
    p.price_rank_in_category

FROM {{ ref('mart_sales_features') }} s
LEFT JOIN {{ ref('mart_calendar_features') }} c
    ON s.day_id = c.day_id
LEFT JOIN {{ ref('mart_price_features') }} p
    ON  s.item_id          = p.item_id
    AND s.store_id         = p.store_id
    AND c.walmart_year_week = p.walmart_year_week