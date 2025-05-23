insert INTO `//home/students/___/DDS_Eb01`

SELECT
  order_id,
  item_id,
  create_datetime AS valid_from_dttm,
  CASE
    WHEN order_status = 'deleted' THEN Datetime("2105-12-31T00:00:00Z")
    WHEN next_dt IS NULL THEN Datetime("2105-12-31T00:00:00Z")
    ELSE next_dt
  END AS valid_to_dttm,
  cyr,
  item_count,
  item_price,
  order_status,
  sku,
  user_id
FROM (
  SELECT
    order_id,
    item_id,
    create_datetime,
    LEAD(create_datetime) OVER (PARTITION BY order_id, item_id ORDER BY create_datetime) AS next_dt,
    cyr,
    item_count,
    item_price,
    order_status,
    sku,
    user_id
  FROM `//home/students/___h/ODS_Eb01`
) AS sub

ORDER BY order_id, item_id, valid_from_dttm, valid_to_dttm;