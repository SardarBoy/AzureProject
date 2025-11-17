import dlt

@dlt.table
def factstream_stg():
    df = spark.readStream.table("spotify_catalogue.silver.factstream")
    return df 

dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
    target="factstream",          # Replace with your target table
    source="factstream_stg",         # Replace with your source
    keys=["stream_id"],      # List of primary or natural keys
    sequence_by="stream_timestamp",      # Column to determine sequence/order          # 
    stored_as_scd_type=1,               # Example: SCD Type 1 or 2
    track_history_except_column_list=None, # Optional: exclude columns from history
    name="my_cdc_flow_factstream",                 # Optional name of the flow
    once=False                           # Run once or continuously
)
