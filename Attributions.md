```sql
WITH
  WebsiteUserStreaming AS (
  SELECT "11111" AS userid, "2020-01-01" AS time, "blog" AS source, "zero" AS event UNION ALL
  SELECT "11111", "2020-01-02", "organic", "CONVERSION" UNION ALL
  SELECT "22222", "2020-01-01", "ppc", "zero" UNION ALL
  SELECT "22222", "2020-01-02", "blog", "zero" UNION ALL
  SELECT "22222", "2020-01-03", "organic", "zero" UNION ALL
  SELECT "22222", "2020-01-04", "ppc", "zero" UNION ALL
  SELECT "22222", "2020-01-05", "organic", "zero" UNION ALL
  SELECT "22222", "2020-01-06", "direct", "zero" UNION ALL
  SELECT "22222", "2020-01-07", "direct", "CONVERSION" UNION ALL
  SELECT "66666", "2020-01-01", "ppc", "CONVERSION" UNION ALL
  SELECT "66666", "2020-01-02", "blog", "zero" UNION ALL
  SELECT "66666", "2020-01-03", "ppc", "zero"),
FormatedStreaming AS (
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
    WebsiteUserStreaming)

```
