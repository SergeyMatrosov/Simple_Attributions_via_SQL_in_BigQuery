```sql
WITH
  WebsiteUserStreaming AS (
  SELECT "11111" AS userid, "2020-01-01" AS time, "blog" AS source, "zero" AS event UNION ALL
  SELECT "11111", "2020-01-02", "organic", "CONVERSION" UNION ALL
  --added here rows with another two orders by user '11111'
  SELECT "11111", "2020-01-03", "organic", "CONVERSION" UNION ALL
  SELECT "11111", "2020-01-04", "blog", "CONVERSION" UNION ALL
  SELECT "22222", "2020-01-01", "ppc", "zero" UNION ALL
  SELECT "22222", "2020-01-02", "blog", "zero" UNION ALL
  SELECT "22222", "2020-01-03", "organic", "zero" UNION ALL
  SELECT "22222", "2020-01-04", "ppc", "zero" UNION ALL
  SELECT "22222", "2020-01-05", "organic", "zero" UNION ALL
  SELECT "22222", "2020-01-06", "direct", "zero" UNION ALL
  SELECT "22222", "2020-01-07", "direct", "CONVERSION" UNION ALL
  --added here two rows: one with an order and another one is without by user '22222'
  SELECT "22222", "2020-01-08", "direct", "zero" UNION ALL
  SELECT "22222", "2020-01-09", "direct", "CONVERSION" UNION ALL
  SELECT "66666", "2020-01-01", "blog", "zero" UNION ALL
  SELECT "66666", "2020-01-02", "ppc", "CONVERSION" UNION ALL
  SELECT "66666", "2020-01-03", "blog", "zero" UNION ALL
  SELECT "66666", "2020-01-04", "ppc", "zero" UNION ALL
  --added here a row with second order by user '66666'
  SELECT "66666", "2020-01-05", "blog", "CONVERSION"),
  FormatedStreaming AS (
  SELECT
  userid,
  CASE
  WHEN event = 0 AND total_orders = 0 THEN CONCAT('order', '-', CAST(total_orders + 1 AS STRING))
  WHEN event = 0 AND total_orders = 1 THEN CONCAT('order', '-', CAST(total_orders+1 AS STRING))
  WHEN event - total_orders < 0 THEN CONCAT('order', '-', CAST(total_orders AS STRING))
  ELSE CONCAT('order', '-', CAST(total_orders AS STRING))
  END AS order_num,
  date_in_sec,
  source,
  event,
  total_orders
  FROM(
    SELECT
    *,
    --running total (others call it cumulative sum)
    SUM(event) OVER (PARTITION BY userid ORDER BY date_in_sec ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_orders
    FROM(
    SELECT
      userid,
      UNIX_SECONDS(TIMESTAMP(time)) AS date_in_sec,
      source,
      CASE
        WHEN event = 'zero' THEN 0
        WHEN event = 'CONVERSION' THEN 1
    END
      AS event
    FROM
      WebsiteUserStreaming)))
--TO TEST ATTRIBUTION, JUST _COMMENT_ FIRST '/*' ABOVE OF ITS NAME.
--YOU CAN USE ONLY ONE OF THESE CODE BLOCKS BELOW AT ONCE! OTHERWISE, IT WON'T WORK.

--EXAMPLE:
--TABLE FormatedStreaming
--/* <-uncommented
SELECT
*
FROM
FormatedStreaming
--*/

/*
--LAST_TOUCH
SELECT
  userid,
  date_in_sec,
  source,
  CASE
    WHEN event > 0 AND date_in_sec > LAG(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN '1'
    WHEN event > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN '1'
  ELSE
  'null'
END
  AS last_touch_attribution,
FROM
  FormatedStreaming
*/

/*
--NOT WORKING AS EXPECTED IN THIS CASE
--DON'T KNOW HOW TO HANDLE YET
--LAST_NOTOUCH_NON_DIRECT
SELECT
  userid,
  order_num,
  date_in_sec,
  source,
  CASE
    WHEN event > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN '1'
    WHEN event = 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN 'null'
    WHEN event = 0 AND LAG(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN 'null'
    WHEN SUM(event) OVER (PARTITION BY userid, order_num) > 0 AND date_in_sec < LAST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    AND LEAD(source) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) = 'direct' AND source != 'direct' THEN '1'
    WHEN event > 0 AND date_in_sec = LAST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AND source != 'direct' THEN '1'
  ELSE
  'null'
END
  AS last_non_direct
FROM
  FormatedStreaming
--*/

/*
--FIRST_TOUCH
SELECT
  userid,
  date_in_sec,
  source,
  CASE
    WHEN MAX(event) OVER (PARTITION BY userid, order_num) = 1 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN '1'
  ELSE
  'null'
END
  AS first_touch_attribution
FROM
  FormatedStreaming
--*/

/*
--ANY TOUCH
SELECT
  userid,
  date_in_sec,
  source,
  CASE
    WHEN MAX(event) OVER (PARTITION BY userid) = 1 THEN '1'
  ELSE
  'null'
END
  AS any_touch_attribution
FROM
  FormatedStreaming
--*/

/*
--ALL CREDIT TO BLOG ONLY
SELECT
  userid,
  date_in_sec,
  source,
  CASE
    WHEN MAX(event) OVER (PARTITION BY userid, order_num) = 1 AND source = 'blog' THEN '1'
  ELSE
  'null'
END
  AS blog_only
FROM
  FormatedStreaming
--*/

/*
--BLOG + LAST TOUCH
SELECT
  userid,
  date_in_sec,
  source,
  CASE
    WHEN MAX(event) OVER (PARTITION BY userid, order_num) = 1 AND source = 'blog' THEN '1'
    WHEN event > 0 AND date_in_sec > LAG(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN '1'
    WHEN event > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) THEN '1'
  ELSE
  'null'
END
  AS blog_and_last
FROM
  FormatedStreaming
--*/

/*
--TIME_DECAY
SELECT
userid,
date_in_sec,
source,
ROUND(IF(SAFE_CAST(weights AS FLOAT64)=0 OR SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY userid, order_num)=0, 0, SAFE_CAST(weights AS FLOAT64)/SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY userid, order_num)), 2) AS time_decay,
FROM
(SELECT
  userid,
  order_num,
  date_in_sec,
  source,
  CASE
    WHEN date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND MAX(event) OVER (PARTITION BY userid) = 1 THEN SAFE_CAST(1.1-ROW_NUMBER() OVER (PARTITION BY userid, order_num) AS STRING)
    WHEN date_in_sec > LAG(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND MAX(event) OVER (PARTITION BY userid, order_num) = 1 THEN SAFE_CAST(ROUND(1.1-1/ROW_NUMBER() OVER (PARTITION BY userid, order_num), 2) AS STRING)
  ELSE
  'null'
END
  AS weights
FROM
  FormatedStreaming)
--*/

/*
--TIME_DECAY_REVERSED
SELECT
userid,
date_in_sec,
source,
ROUND(IF(SAFE_CAST(weights AS FLOAT64)=0 OR SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY userid, order_num)=0, 0, SAFE_CAST(weights AS FLOAT64)/SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY userid, order_num)), 2) AS time_decay_reversed,
FROM
(SELECT
  userid,
  order_num,
  date_in_sec,
  source,
  CASE
    WHEN date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND MAX(event) OVER (PARTITION BY userid, order_num) = 1 THEN SAFE_CAST(ROW_NUMBER() OVER (PARTITION BY userid, order_num) AS STRING)
    WHEN date_in_sec > LAG(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND MAX(event) OVER (PARTITION BY userid, order_num) = 1 THEN SAFE_CAST(ROUND(1/ROW_NUMBER() OVER (PARTITION BY userid, order_num), 2) AS STRING)
  ELSE
  'null'
END
  AS weights
FROM
  FormatedStreaming)
--*/

/*
--POSITION BASED
SELECT
userid,
date_in_sec,
source,
CASE 
WHEN SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY userid, order_num) > 1 THEN SAFE_CAST(ROUND(SAFE_CAST(weights AS FLOAT64)/SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY userid, order_num), 2) AS STRING)
ELSE weights
END AS position_based
FROM
(
SELECT
  userid,
  order_num,
  date_in_sec,
  source,
  
  CASE
  --one channel and instant conversion
  WHEN MAX(event) OVER (PARTITION BY userid, order_num) > 0 AND COUNT(userid) OVER (PARTITION BY userid, order_num) = 1 THEN '1'
  
  WHEN MAX(event) OVER (PARTITION BY userid, order_num) > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND COUNT(userid) OVER (PARTITION BY userid, order_num) > 2 
  THEN SAFE_CAST(ROUND(COUNT(userid) OVER (PARTITION BY userid, order_num)*0.4, 3) AS STRING)
  
  WHEN MAX(event) OVER (PARTITION BY userid, order_num) > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND COUNT(userid) OVER (PARTITION BY userid) = 2 THEN '0.5'
  WHEN MAX(event) OVER (PARTITION BY userid, order_num) > 0 AND date_in_sec != FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND date_in_sec != LAST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  AND COUNT(userid) OVER (PARTITION BY userid, order_num) > 2 THEN SAFE_CAST(ROUND(COUNT(userid) OVER (PARTITION BY userid, order_num)*0.2, 1) AS STRING)
  
  WHEN MAX(event) OVER (PARTITION BY userid, order_num) > 0 AND date_in_sec = LAST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND COUNT(userid) OVER (PARTITION BY userid, order_num) > 2 
  THEN SAFE_CAST(ROUND(COUNT(userid) OVER (PARTITION BY userid, order_num)*0.4, 1) AS STRING)
  
  WHEN MAX(event) OVER (PARTITION BY userid, order_num) > 0 AND date_in_sec = LAST_VALUE(date_in_sec) OVER (PARTITION BY userid, order_num ORDER BY date_in_sec) AND COUNT(userid) OVER (PARTITION BY userid, order_num) = 2 THEN '0.5'
  ELSE '0'
  END AS weights
FROM
  FormatedStreaming)
--*/
```
