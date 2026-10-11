-- What will loading the staged rows do to curated_orders?  Run BEFORE load_curated.sql.
-- "Changed" uses the same predicate as load_curated.sql (see the upsert policy there).
-- Named parameters: :has_channel, :has_extra (1 when the file actually carries that column).
SELECT
    COALESCE(SUM(CASE WHEN c.order_id IS NULL THEN 1 ELSE 0 END), 0) AS inserted,
    COALESCE(SUM(CASE WHEN c.order_id IS NOT NULL AND (
                   c.customer_id IS NOT s.customer_id
                OR c.order_date  IS NOT s.order_date
                OR c.amount      IS NOT s.amount
                OR c.status      IS NOT s.status
                OR (:has_channel AND c.channel    IS NOT s.channel)
                OR (:has_extra   AND c.extra_json IS NOT s.extra_json)
           ) THEN 1 ELSE 0 END), 0)                                    AS updated,
    COALESCE(SUM(CASE WHEN c.order_id IS NOT NULL AND NOT (
                   c.customer_id IS NOT s.customer_id
                OR c.order_date  IS NOT s.order_date
                OR c.amount      IS NOT s.amount
                OR c.status      IS NOT s.status
                OR (:has_channel AND c.channel    IS NOT s.channel)
                OR (:has_extra   AND c.extra_json IS NOT s.extra_json)
           ) THEN 1 ELSE 0 END), 0)                                    AS unchanged
FROM stg_orders AS s
LEFT JOIN curated_orders AS c ON c.order_id = s.order_id;
