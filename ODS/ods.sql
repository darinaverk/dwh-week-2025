
INSERT INTO `//home/students/___/ODS_Tab02` (
    create_datetime,
    order_id,
    order_status,
    user_id,
    item_id,
    cyr,
    item_count,
    item_price,
    sku
)
SELECT
    DateTime::MakeDatetime(
        DateTime::ParseIso8601(
            Yson::ConvertToString(Yson::Lookup(data, "datetime"))
        )
    ) AS create_datetime,

    COALESCE(
        Yson::ConvertTo(Yson::Lookup(data, "id"), Int64),
        0
    ) AS order_id,

    COALESCE(
        Yson::ConvertToString(Yson::Lookup(data, "status")),
        "unknown"
    ) AS order_status,

    COALESCE(
        Yson::ConvertTo(Yson::Lookup(data, "user_id"), Int64),
        0
    ) AS user_id,

    Yson::ConvertTo(Yson::Lookup(item_list, "id"), Int64?) AS item_id,
    Yson::ConvertTo(Yson::Lookup(item_list, "cyr"), String?) AS cyr,
    Yson::ConvertTo(Yson::Lookup(item_list, "count"), Int64?) AS item_count,
    Yson::ConvertTo(Yson::Lookup(item_list, "price"), Int64?) AS item_price,
    Yson::ConvertTo(Yson::Lookup(item_list, "sku"), Int64?) AS sku

FROM (
    SELECT
        data,
        COALESCE(
            Yson::ConvertToList(Yson::Lookup(data, "item_list")),
            []
        ) AS items
    FROM `//home/homework_task_dwh/order_event`
)
FLATTEN BY items AS item_list;


COMMIT;

INSERT INTO @window
SELECT
  t.*,
  LEAD(t.create_datetime) OVER (
    PARTITION BY t.order_id, t.item_id
    ORDER BY t.create_datetime
  ) AS next_item_date,
  MAX(t.create_datetime) OVER (
    PARTITION BY t.order_id
  ) AS last_order_date,
  MAX(t.create_datetime) OVER (
    PARTITION BY t.order_id, t.item_id
  ) AS last_item_date
FROM `//home/students/___/ODS_Tab02` AS t;
COMMIT;

INSERT INTO @deleted_dates
SELECT
  w1.order_id as order_id,
  w1.item_id as item_id,
  MIN(w2.create_datetime) AS deleted_datetime
FROM @window AS w1
JOIN @window AS w2
  ON w1.order_id = w2.order_id
WHERE
  w1.next_item_date IS NULL
  AND w1.create_datetime < w1.last_order_date
  --AND w2.order_status not in ('completed', 'cancelled') #это не нужно
  AND w2.create_datetime > w1.last_item_date
  AND w2.create_datetime <= w1.last_order_date
  AND w2.user_id=w1.user_id

GROUP BY
  w1.order_id,
  w1.item_id;
COMMIT;

INSERT INTO `//home/students/___/ODS_Tab02` (
    item_id,
    create_datetime,
    cyr,
    item_count,
    item_price,
    order_id,
    order_status,
    sku,
    user_id
)
SELECT
    d.item_id           AS item_id,
    d.deleted_datetime AS create_datetime,

  NULL                 AS cyr,
  NULL                 AS item_count,
  NULL                 AS item_price,
  d.order_id          AS order_id,
  "deleted"            AS order_status,
  NULL                 AS sku,
  l.user_id           AS user_id


FROM @deleted_dates AS d
JOIN (
  SELECT
    sub.order_id,
    sub.item_id,
    sub.user_id
  FROM (
    SELECT
      t01.order_id,
      t01.item_id,
      t01.user_id,
      ROW_NUMBER() OVER (
        PARTITION BY t01.order_id, t01.item_id
        ORDER BY t01.create_datetime DESC
      ) AS rk
    FROM `//home/students/____/ODS_Tab02` AS t01
  ) AS sub
  WHERE sub.rk = 1

) AS l
  ON d.order_id = l.order_id
 AND d.item_id  = l.item_id

ORDER BY
  item_id;

COMMIT;
