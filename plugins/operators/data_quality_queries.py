null_pct_check = """WITH
    Q AS (
             SELECT
                 SUM(CASE WHEN {column_name} IS NULL THEN 1 END) :: FLOAT     AS NULL_CNT
               , SUM(CASE WHEN {column_name} IS NOT NULL THEN 1 END) :: FLOAT AS NON_NULL_CNT
               , SUM(1) :: FLOAT                                       AS CNT
             FROM
                 {table_name}
    )
SELECT
    NULL_CNT / CNT AS NULL_PCT
FROM
    Q
WHERE
    NULL_CNT / CNT > {threshold}"""

duplicate_check = """WITH
    Q AS (
             SELECT
                 {column_name}
               , COUNT(1) A
             FROM
                 {table_name}
             GROUP BY {column_name}
             HAVING
                 A > 1
    )
SELECT *
FROM
    Q"""

row_count_check = """
SELECT
    COUNT(1) c
FROM
     {table_name}
HAVING
    c = 0
"""