-- Step 1: Identify all (user_id, order_id, sku) that ever had status = 'cancelled'
INSERT INTO @to_delete
SELECT DISTINCT
  user_id,
  order_id,
  sku
FROM `home/students/darverhovyh/DDS_Eb01`
WHERE order_status = 'cancelled';
--and valid_from_dttm >  DATETIME('2025-02-01T00:00:00Z');
COMMIT;

-- Step 2: Exclude all rows for those cancelled SKUs/orders; keep only RUB
INSERT INTO @excl_ta
SELECT s.*
FROM `home/students/darverhovyh/DDS_Eb01` AS s
LEFT JOIN @to_delete AS d
  ON s.user_id  = d.user_id
 AND s.order_id = d.order_id
 AND s.sku      = d.sku
WHERE d.sku        IS NULL
  AND s.user_id    IS NOT NULL
  AND s.order_id   IS NOT NULL
  AND s.sku        IS NOT NULL;
  --AND s.cyr        = 'RUB';
COMMIT;

-- Step 3: Truncate each interval at the cutoff date 2025-02-01
INSERT INTO @filtered_date
SELECT
  user_id,
  cyr,
  item_id,
  item_price,
  item_count,
  order_id,
  order_status,
  sku,
  valid_from_dttm,
  CASE
    WHEN valid_to_dttm <= DATETIME('2025-02-01T00:00:00Z') THEN valid_to_dttm
    WHEN valid_from_dttm >  DATETIME('2025-02-01T00:00:00Z') THEN NULL
    ELSE                              DATETIME('2025-02-01T00:00:00Z')
  END AS last_date
FROM @excl_ta
WHERE valid_from_dttm <= DATETIME('2025-02-01T00:00:00Z');
COMMIT;

-- Step 4: Compute first_success_date per SKU (first time status  ('pay','completed','delivered'))
insert into @first_success
  SELECT
    user_id,
    order_id,
    sku,
    MIN(valid_from_dttm) AS first_success_date
  FROM @filtered_date
  WHERE order_status IN ('pay','completed','delivered')
  GROUP BY user_id, order_id, sku;

commit;
-- Step 5: Rank each segment to find the very first and very last per SKU
INSERT INTO @ordered
SELECT
  fd.user_id as user_id,
  fd.order_id as order_id,
  fd.sku as sku,
  fd.item_count as item_count,
  fd.item_price as item_price,
  fd.order_status as order_status,
  fd.valid_from_dttm as valid_from_dttm,
  fd.last_date as last_date,
  fs.first_success_date as first_success,
  ROW_NUMBER() OVER (
    PARTITION BY fd.user_id, fd.order_id, fd.sku
    ORDER BY fd.valid_from_dttm
  ) AS rn_start,
  ROW_NUMBER() OVER (
    PARTITION BY fd.user_id, fd.order_id, fd.sku
    ORDER BY fd.last_date DESC
  ) AS rn_end
FROM @filtered_date AS fd
JOIN @first_success    AS fs
  ON fd.user_id  = fs.user_id
 AND fd.order_id = fs.order_id
 AND fd.sku      = fs.sku
WHERE fd.last_date IS NOT NULL;
COMMIT;

-- Step 6: Extract the start & end segments only where final status  ('pay','completed','delivered')
INSERT INTO @step_tocount

SELECT
  o_st.user_id         AS user_id,
  o_st.item_count      AS item_count,
  o_st.item_price      AS item_price,
  o_st.order_id        AS order_id,
  o_st.sku             AS sku,
  o_st.order_status    AS start_status,
  o_st.valid_from_dttm AS start_date,
  o_end.order_status      AS end_status,
  o_end.last_date         AS end_date
FROM @ordered  o_st
JOIN @ordered  o_end
  ON o_st.user_id  = o_end.user_id
 AND o_st.order_id = o_end.order_id
 AND o_st.sku      = o_end.sku
WHERE
  o_st.rn_start = 1
  AND o_end.rn_end   = 1
  AND o_end.order_status IN ('pay','completed','delivered');
COMMIT;

-- Step 7: Total count of paid items per user
INSERT INTO @item_count
SELECT
  user_id,
  SUM(item_count) AS count_pay_item
FROM @step_tocount
GROUP BY user_id;
COMMIT;

-- Step 8: Compute revenue per SKU and pick best_sku for each user
INSERT INTO @revenue_per_item
SELECT
  user_id,
  sku,
  item_count * item_price AS position_revenue,
  item_count
FROM @step_tocount;
COMMIT;

INSERT INTO @sku_ranked
SELECT
  user_id,
  sku,
  position_revenue,
  ROW_NUMBER() OVER (
    PARTITION BY user_id
    ORDER BY position_revenue DESC
  ) AS rn
FROM @revenue_per_item;
COMMIT;

INSERT INTO @best_sku
SELECT
  user_id,
  sku           AS best_sku,
  position_revenue AS best_sku_price
FROM @sku_ranked
WHERE rn = 1;
COMMIT;

-- Step 9: Average order completion day across all non-cancelled orders
insert into @order_bounds
  SELECT
    user_id,
    order_id,
    sku,
    MIN(valid_from_dttm) AS start_ts,
    MAX(last_date)       AS end_ts
  FROM @filtered_date
  GROUP BY user_id, order_id, sku;

commit;

insert into @order_durations
  SELECT
    user_id,
    (DateTime::ToSeconds(end_ts) - DateTime::ToSeconds(start_ts)) / 86400.0 AS duration_days
  FROM @order_bounds
  WHERE end_ts IS NOT NULL;

commit;

INSERT INTO @avg_durs
SELECT
  user_id,
  AVG(duration_days) AS avg_order_completion_day
FROM @order_durations
GROUP BY user_id;
COMMIT;

-- Final Step: Populate CDM_E02 with all metrics, sorted by user_id
INSERT INTO `home/students/darverhovyh/CDM_E03` (
 user_id,
 count_pay_item,
 best_sku,
 best_sku_price,
  avg_order_completion_day
)
SELECT
  ic.user_id                  AS user_id,
  ic.count_pay_item           AS count_pay_item,
  bs.best_sku                 AS best_sku,
  bs.best_sku_price           AS best_sku_price,
  ad.avg_order_completion_day AS avg_order_completion_day
FROM @item_count AS ic
JOIN @best_sku   AS bs ON ic.user_id = bs.user_id
JOIN @avg_durs   AS ad ON ic.user_id = ad.user_id
ORDER BY ic.user_id;


