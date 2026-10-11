-- Set-based, idempotent upsert from staging into the curated table.
--
-- Upsert policy (important under schema drift):
--   * Re-loading identical data is a no-op (the WHERE on DO UPDATE skips unchanged rows).
--   * A column that is ABSENT from the file is "not provided" and never overwrites existing data.
--     A column that is present but empty DOES overwrite (the producer explicitly sent null).
--     This stops an upstream that drops an optional column from silently nulling history.
--
-- Named parameters: :file_id, :now, :has_channel, :has_extra
INSERT INTO curated_orders (
    order_id, customer_id, order_date, amount, status, channel, extra_json,
    source_file_id, first_loaded_at, last_updated_at)
SELECT
    s.order_id, s.customer_id, s.order_date, s.amount, s.status, s.channel, s.extra_json,
    :file_id, :now, :now
FROM stg_orders AS s
WHERE true
ON CONFLICT (order_id) DO UPDATE SET
    customer_id     = excluded.customer_id,
    order_date      = excluded.order_date,
    amount          = excluded.amount,
    status          = excluded.status,
    channel         = CASE WHEN :has_channel THEN excluded.channel    ELSE curated_orders.channel    END,
    extra_json      = CASE WHEN :has_extra   THEN excluded.extra_json ELSE curated_orders.extra_json END,
    source_file_id  = excluded.source_file_id,
    last_updated_at = excluded.last_updated_at
WHERE curated_orders.customer_id IS NOT excluded.customer_id
   OR curated_orders.order_date  IS NOT excluded.order_date
   OR curated_orders.amount      IS NOT excluded.amount
   OR curated_orders.status      IS NOT excluded.status
   OR (:has_channel AND curated_orders.channel    IS NOT excluded.channel)
   OR (:has_extra   AND curated_orders.extra_json IS NOT excluded.extra_json);
