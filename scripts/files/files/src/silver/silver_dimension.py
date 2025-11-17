# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
project_pth=(os.path.join(os.getcwd(),'..','..'))
sys.path.append(project_pth)
from utils.transformations import *
from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC **DimUser**

# COMMAND ----------

df=spark.read.format("parquet").load("abfss://bronze@storagegurtej.dfs.core.windows.net/DimUser")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@storagegurtej.dfs.core.windows.net/DimUser")


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### AUTOLOADER

# COMMAND ----------

df_user= spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
        .option('cloudFiles.schemaLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimUser/checkpoint')\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storagegurtej.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df_user)

# COMMAND ----------

df_user = df_user.withColumn("user_name",upper(col("user_name")))
display(df_user)

# COMMAND ----------

df_user_obj=  reusable()
df_user=df_user_obj.dropColumns(df_user,['_rescued_data'])
display(df_user)

# COMMAND ----------

df_user=df_user.dropDuplicates(["user_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###DELTA 

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@storagegurtej.dfs.core.windows.net/DimUser/checkpoint")\
        .trigger(once=True)\
            .start("abfss://silver@storagegurtej.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARTIST

# COMMAND ----------

df_art= spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
        .option('cloudFiles.schemaLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimArtist/checkpoint')\
            .load("abfss://bronze@storagegurtej.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df_art_obj= reusable()
df_art=df_art_obj.dropColumns(df_art,['_rescued_data'])
df_art=df_art.dropDuplicates(["artist_id"])
display(df_art)

# COMMAND ----------

# MAGIC %md
# MAGIC MAking tables

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode('append')\
        .option('checkpointLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimUser/checkpoint')\
            .trigger(once=True)\
            .option('path',"abfss://silver@storagegurtej.dfs.core.windows.net/DimUser/data")\
                .toTable("spotify_catalogue.silver.DimUser")

# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode('append')\
        .option('checkpointLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimArtist/checkpoint')\
            .trigger(once=True)\
            .option('path',"abfss://silver@storagegurtej.dfs.core.windows.net/DimArtist/data")\
                .toTable("spotify_catalogue.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DIM TRACK

# COMMAND ----------

df_track= spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
        .option('cloudFiles.schemaLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimTrack/checkpoint')\
            .option('schemaEvolutionMode','addNewColumns')\
            .load("abfss://bronze@storagegurtej.dfs.core.windows.net/DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ###transformation

# COMMAND ----------

df_track=df_track.withColumn('durationFlag',when(col('duration_sec')<150,'low')\
    .when(col('duration_sec')<300,'medium')\
    .otherwise('high'))

df_track=df_track.withColumn('track_name',regexp_replace(col('track_name'),'-',' '))
df_track=reusable().dropColumns(df_track,['_rescued_data'])
display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode('append')\
        .option('checkpointLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimTrack/checkpoint')\
            .trigger(once=True)\
            .option('path',"abfss://silver@storagegurtej.dfs.core.windows.net/DimTrack/data")\
                .toTable("spotify_catalogue.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dim date

# COMMAND ----------

df_date= spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
        .option('cloudFiles.schemaLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimDate/checkpoint')\
            .option('schemaEvolutionMode','addNewColumns')\
            .load("abfss://bronze@storagegurtej.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date=reusable().dropColumns(df_date,['_rescued_data'])
display(df_date)

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode('append')\
        .option('checkpointLocation','abfss://silver@storagegurtej.dfs.core.windows.net/DimDate/checkpoint')\
            .trigger(once=True)\
            .option('path',"abfss://silver@storagegurtej.dfs.core.windows.net/DimDate/data")\
                .toTable("spotify_catalogue.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fact stream

# COMMAND ----------

df_fact= spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
        .option('cloudFiles.schemaLocation','abfss://silver@storagegurtej.dfs.core.windows.net/FactStream/checkpoint')\
            .option('schemaEvolutionMode','addNewColumns')\
            .load("abfss://bronze@storagegurtej.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fact=reusable().dropColumns(df_fact,['_rescued_data'])
display(df_fact)

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode('append')\
        .option('checkpointLocation','abfss://silver@storagegurtej.dfs.core.windows.net/FactStream/checkpoint')\
            .trigger(once=True)\
            .option('path',"abfss://silver@storagegurtej.dfs.core.windows.net/FactStream/data")\
                .toTable("spotify_catalogue.silver.FactStream")