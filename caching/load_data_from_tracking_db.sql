SELECT data_sources                                                          AS DATA_SOURCE,
    ARRAY_COMPACT(ARRAY_APPEND(FIELDS,COMP_FIELDS))                          AS ALL_FIELDSS,
    SUM(TIME_ELAPSED)/1000                                                   AS TOTAL_TIME_ELAPSED_S,
    COUNT(CREATED_AT)                                                        AS TOTAL_QUERIES,
    COUNT(IFF(TIME_ELAPSED > 20000,CREATED_AT,NULL))                         AS TOTAL_QUERIES_OVER_20S,
    SUM(TIME_ELAPSED)/1000/COUNT(CREATED_AT)                                 AS AVG_QUERY_TIME_S,
    MAX(TIME_ELAPSED)/1000                                                   AS MAX_QUERY_TIME_S,
    MIN(TIME_ELAPSED)/1000                                                   AS MIN_QUERY_TIME_S
FROM (
    SELECT
        INFO:request:subrequests[0]:properties:data_sources                                           AS DATA_SOURCES,
        ARRAY_COMPACT(ARRAY_APPEND(INFO:request:subrequests[0]:fields, INFO:data_request:fields))     AS FIELDS,
        ARRAY_COMPACT(INFO:request:comparison:fields)                                                      AS COMP_FIELDS,
        INFO:metrics_internal:executor_time_elapsed                   AS TIME_ELAPSED,
        CREATED_AT                                                          AS CREATED_AT
    FROM "DATABASE"."SCHEMA"."TABLE"
    WHERE INFO:request:origin:system = 'production_data'
        AND created_at > '2020-01-01'
        AND INFO:status = 'successful'
) AS q0
GROUP BY 1,2
HAVING TOTAL_QUERIES > 50
    AND TOTAL_QUERIES_OVER_20S > 5
ORDER BY 1,6 DESC
;
