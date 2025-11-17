import dlt

@dlt.table
def dimtrack_stg():
    df = spark.readStream.table("spotify_catalogue.silver.dimtrack")
    return df 

dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
    target="dimtrack",          # Replace with your target table
    source="dimtrack_stg",         # Replace with your source
    keys=["track_id"],      # List of primary or natural keys
    sequence_by="updated_at",      # Column to determine sequence/order          # 
    stored_as_scd_type=2,               # Example: SCD Type 1 or 2
    track_history_except_column_list=None, # Optional: exclude columns from history
    name="my_cdc_flow_dimtrack",                 # Optional name of the flow
    once=False                           # Run once or continuously
)
