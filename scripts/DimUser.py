import dlt

expectations  = {
    "rule1":"user_id IS NOT NULL"
}
@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    df = spark.readStream.table("spotify_catalogue.silver.dimuser")
    return df 

dlt.create_streaming_table(
    name="dimuser",
    expect_all_or_drop=expectations
)

dlt.create_auto_cdc_flow(
    target="dimuser",          # Replace with your target table
    source="dimuser_stg",         # Replace with your source
    keys=["user_id"],      # List of primary or natural keys
    sequence_by="updated_at",      # Column to determine sequence/order          # 
    stored_as_scd_type=2,               # Example: SCD Type 1 or 2
    track_history_except_column_list=None, # Optional: exclude columns from history
    name="my_cdc_flow",                 # Optional name of the flow
    once=False                           # Run once or continuously
)
