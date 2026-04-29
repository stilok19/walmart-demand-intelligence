WITH price_stats AS (
    SELECT
        store_id,
        item_id,
        walmart_year_week,
        sell_price,
        price_change,
        price_change_pct,
        price_rank_in_category,

        -- 4-week rolling average price
        AVG(sell_price) OVER (
            PARTITION BY store_id, item_id
            ORDER BY walmart_year_week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        )                                       AS avg_price_rolling_4wk,

        -- 4-week price momentum
        sell_price - LAG(sell_price, 4) OVER (
            PARTITION BY store_id, item_id
            ORDER BY walmart_year_week
        )                                       AS price_momentum_4wk,

        -- Is price on promotion (> 5% discount from 4wk avg)
        CASE
            WHEN sell_price < AVG(sell_price) OVER (
                PARTITION BY store_id, item_id
                ORDER BY walmart_year_week
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ) * 0.95
            THEN 1 ELSE 0
        END                                     AS is_on_promotion,

        -- Price relative to category average ex. FOODS_1
        SAFE_DIVIDE(
            sell_price,
            AVG(sell_price) OVER (
                PARTITION BY
                    SPLIT(item_id, '_')[OFFSET(0)],
                    SPLIT(item_id, '_')[OFFSET(1)],
                    store_id,
                    walmart_year_week
            )
        )                                       AS relative_price_in_category

    FROM {{ ref('stg_sell_prices') }}
)

SELECT * FROM price_stats