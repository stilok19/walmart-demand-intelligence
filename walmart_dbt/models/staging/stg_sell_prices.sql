SELECT
    store_id,
    item_id,
    CAST(wm_yr_wk AS INT64)             AS walmart_year_week,
    CAST(sell_price AS FLOAT64)         AS sell_price,

    -- Price change from previous day
    CAST(sell_price AS FLOAT64) -
    LAG(CAST(sell_price AS FLOAT64))
    OVER (
        PARTITION BY store_id, item_id
        ORDER BY CAST(wm_yr_wk AS INT64)
    )                                   AS price_change,

    -- Price change percentage from previos day
    SAFE_DIVIDE(
        CAST(sell_price AS FLOAT64) -
        LAG(CAST(sell_price AS FLOAT64))
        OVER (
            PARTITION BY store_id, item_id
            ORDER BY CAST(wm_yr_wk AS INT64)
        ),
        LAG(CAST(sell_price AS FLOAT64))
        OVER (
            PARTITION BY store_id, item_id
            ORDER BY CAST(wm_yr_wk AS INT64)
        )
    ) * 100                             AS price_change_pct,

    -- Price rank within category ex. FOODS_1 (relative price)
    RANK() OVER (
        PARTITION BY
            SPLIT(item_id, '_')[OFFSET(0)],
            SPLIT(item_id, '_')[OFFSET(1)],
            store_id,
            CAST(wm_yr_wk AS INT64)
        ORDER BY CAST(sell_price AS FLOAT64)
    )                                   AS price_rank_in_category

FROM {{ source('walmart_raw', 'raw_sell_prices') }}
WHERE sell_price IS NOT NULL