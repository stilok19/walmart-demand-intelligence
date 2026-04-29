WITH base AS (
    SELECT
        s.item_id,
        s.store_id,
        s.state,
        s.dept_id,
        s.day_id,
        s.day_num,
        s.category,
        s.department,
        s.store_number,
        s.sales,
        s.is_no_sales
    FROM {{ ref('stg_sales') }} s
),

lag_features AS (
    --within same item and same store
    SELECT
        *,

        -- Lag features
        LAG(sales, 1) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
        )                                       AS lag_1,

        LAG(sales, 7) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
        )                                       AS lag_7,

        LAG(sales, 14) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
        )                                       AS lag_14,

        LAG(sales, 28) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
        )                                       AS lag_28,

        LAG(sales, 35) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
        )                                       AS lag_35,
        -- Rolling mean features
        AVG(sales) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        )                                       AS rolling_mean_7,

        AVG(sales) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
        )                                       AS rolling_mean_28,

        AVG(sales) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN 56 PRECEDING AND 1 PRECEDING
        )                                       AS rolling_mean_56,

        -- Rolling std features (demand volatility)
        STDDEV(sales) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        )                                       AS rolling_std_7,

        STDDEV(sales) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
        )                                       AS rolling_std_28,

        -- Rolling max (peak demand)
        MAX(sales) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
        )                                       AS rolling_max_28,

        -- no sales streak (consecutive days with no sales)
        COUNTIF(sales = 0) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        )                                       AS no_sales_last_7_days,

        -- Cumulative sales
        SUM(sales) OVER (
            PARTITION BY item_id, store_id
            ORDER BY day_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                       AS cumulative_sales

    FROM base
)

SELECT * FROM lag_features