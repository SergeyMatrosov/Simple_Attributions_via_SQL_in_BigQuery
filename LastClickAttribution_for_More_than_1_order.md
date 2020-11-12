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
  SELECT "66666", "2020-01-01", "ppc", "CONVERSION" UNION ALL
  SELECT "66666", "2020-01-02", "blog", "zero" UNION ALL
  SELECT "66666", "2020-01-03", "ppc", "zero" UNION ALL
  --added here a row with second order by user '66666'
  SELECT "66666", "2020-01-04", "blog", "CONVERSION"),
  FormatedStreaming AS (
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
    WebsiteUserStreaming))
--TO TEST ATTRIBUTION, JUST _COMMENT_ FIRST '/*' ABOVE OF ITS NAME.
--YOU CAN USE ONLY ONE OF THESE CODE BLOCKS BELOW AT ONCE! OTHERWISE, IT WON'T WORK.

--EXAMPLE:
--TABLE WebsiteUserStreaming
/* <-uncommented
SELECT
*
FROM
FormatedStreaming
*/

--/*
--LAST_TOUCH
SELECT
  userid,
  date_in_sec,
  source,
  CASE
    WHEN event > 0 AND date_in_sec > LAG(date_in_sec) OVER (PARTITION BY userid ORDER BY date_in_sec) THEN '1'
    WHEN event > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userid ORDER BY date_in_sec) THEN '1'
    --addition condition
    WHEN event != 0 AND total_orders - LAG(total_orders) OVER (PARTITION BY userid ORDER BY date_in_sec) = 1 THEN '1'
  ELSE
  'null'
END
  AS last_touch_attribution
FROM
  FormatedStreaming
--*/
```
