import dlt

@dlt.table
def dimdate_stg():
    df = spark.readStream.table("spotify_catalogue.silver.dimdate")
    return df 

dlt.create_streaming_table("dimdate")

dlt.create_auto_cdc_flow(
    target="dimdate",          # Replace with your target table
    source="dimdate_stg",         # Replace with your source
    keys=["date_key"],      # List of primary or natural keys
    sequence_by="date",      # Column to determine sequence/order          # 
    stored_as_scd_type=2,               # Example: SCD Type 1 or 2
    track_history_except_column_list=None, # Optional: exclude columns from history
    name="my_cdc_flow_dimdate",                 # Optional name of the flow
    once=False                           # Run once or continuously
)
