from pyspark.sql import SparkSession

# Start Spark session
spark = (
    SparkSession.builder.appName("FilterGHJsonPYTHON")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.read.json("../datap/*.json.gz")
df = df.filter(df["type"].isin("IssuesEvent", "IssueCommentEvent"))
df.createOrReplaceTempView("df")

df = spark.sql("""
WITH opened_issues AS (
    SELECT 
        payload.issue.id AS issue_id, 
        MIN(created_at) AS opened_at,  -- Pick first open event per issue
        FIRST(payload.issue.author_association) AS author_association,  
        FIRST(payload.issue.title) AS title,
        FIRST(payload.issue.labels) AS labels,
        FIRST(payload.issue.state) AS state,
        FIRST(payload.issue.state_reason) AS state_reason,
        FIRST(payload.issue.body) AS body
    FROM df
    WHERE type = 'IssuesEvent' 
      AND (payload.action = 'opened' OR payload.action = 'reopened')
    GROUP BY payload.issue.id
), 

closed_issues AS (
    SELECT 
        payload.issue.id AS issue_id, 
        MAX(created_at) AS closed_at  -- Pick last closed event per issue
    FROM df
    WHERE type = 'IssuesEvent' 
      AND payload.action = 'closed'
    GROUP BY payload.issue.id
)

SELECT 
    o.issue_id,
    o.opened_at,
    c.closed_at,
    TIMESTAMPDIFF(SECOND, o.opened_at, c.closed_at) AS time_to_close_seconds,
    o.author_association,
    o.title,
    o.state,
    o.state_reason,
    o.body
FROM opened_issues o
JOIN closed_issues c 
ON o.issue_id = c.issue_id
WHERE c.closed_at >= o.opened_at AND o.state != 'open' -- Ensure closed_at is after opened_at
ORDER BY time_to_close_seconds DESC;
       """)
df.show()
spark.stop()