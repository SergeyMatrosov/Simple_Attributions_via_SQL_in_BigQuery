# Simple Attributions via SQL in BigQuery
Examples of simple attributions for digital marketing efforts for reporting, using SQL in BigQuery:
- Last Touch
- Last Non-Direct Touch
- First Touch
- Any Touch
- Time Decay
- Time Decay Reversed
- Position Based

In case if you using Clickhouse and just want to generate fake table as in example, use this:

'''
--CREATE VIEW IF NOT EXISTS WebsiteUserStreaming AS
SELECT
*
FROM
(WITH '11111' AS userid, '2020-01-01' AS time, 'blog' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '11111' AS userid, '2020-01-02' AS time, 'organic' AS source, 'CONVERSION' AS event
SELECT userid, time, source, event

UNION ALL

WITH '22222' AS userid, '2020-01-01' AS time, 'ppc' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '22222' AS userid, '2020-01-02' AS time, 'blog' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '22222' AS userid, '2020-01-03' AS time, 'organic' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '22222' AS userid, '2020-01-04' AS time, 'ppc' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '22222' AS userid, '2020-01-05' AS time, 'organic' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '22222' AS userid, '2020-01-06' AS time, 'direct' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '22222' AS userid, '2020-01-07' AS time, 'direct' AS source, 'CONVERSION' AS event
SELECT userid, time, source, event

UNION ALL

WITH '66666' AS userid, '2020-01-01' AS time, 'ppc' AS source, 'CONVERSION' AS event
SELECT userid, time, source, event

UNION ALL

WITH '66666' AS userid, '2020-01-02' AS time, 'blog' AS source, 'zero' AS event
SELECT userid, time, source, event

UNION ALL

WITH '66666' AS userid, '2020-01-03' AS time, 'ppc' AS source, 'zero' AS event
SELECT userid, time, source, event)
'''
