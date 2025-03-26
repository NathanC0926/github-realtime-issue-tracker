from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def process_issues(df: DataFrame) -> DataFrame:
    """
    Process the issue event data by cleaning, transforming, and labeling data.
    
    Steps:
    1. Filters out rows where 'issue_closed_at' is null.
    2. Drops irrelevant columns.
    3. Adds 'issue_resolve_time_days' column calculating resolution time in days.
    4. Filters out entries with non-positive resolution times.
    5. Adds 'issue_resolve_time_label' column with resolution time labels.

    Args:
        df (DataFrame): The raw DataFrame containing issue events.

    Returns:
        DataFrame: The processed DataFrame with relevant columns and labels.
    """
    # Step 1: Keep only rows with non-null 'issue_closed_at'
    df = df.where("issue_closed_at is not null")
    
    # Step 2: Drop unnecessary columns
    df = df.drop("event_id","action","author_association","issue_state","issue_state_reason",
                 "issue_comment_count", "issue_updated_at","event_created_at","dim_actor")
    
    # Step 3: Add column 'issue_resolve_time_days'
    df = df.withColumn("issue_resolve_time_days", 
                       F.expr("((cast(issue_closed_at as long) - cast(issue_created_at as long)) / 86400)"))
    
    # Step 4: Filter out rows with non-positive resolution times
    df = df.where("issue_resolve_time_days > 0.0")
    
    # Step 5: Add labeled column 'issue_resolve_time_label'
    df = df.withColumn(
        "issue_resolve_time_label",
        F.expr("""
            CASE 
                WHEN issue_resolve_time_days < (1 / 24) THEN '<1 hour'
                WHEN issue_resolve_time_days >= (1 / 24) AND issue_resolve_time_days < (6 / 24) THEN '1-6 hours'
                WHEN issue_resolve_time_days >= (6 / 24) AND issue_resolve_time_days < 1 THEN '6-24 hours'
                WHEN issue_resolve_time_days >= 1 AND issue_resolve_time_days < 7 THEN '1-7 days'
                WHEN issue_resolve_time_days >= 7 AND issue_resolve_time_days < 28 THEN '1-4 weeks'
                WHEN issue_resolve_time_days >= 28 AND issue_resolve_time_days < 180 THEN '1-6 months'
                WHEN issue_resolve_time_days >= 180 AND issue_resolve_time_days < 365 THEN '6 months - 1 year'
                WHEN issue_resolve_time_days >= 365 THEN '>1 year'
                ELSE 'Unknown'
            END
        """)
    )
    
    return df
