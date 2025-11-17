# Databricks notebook source
pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

parameters=[
    {
        "table":"spotify_catalogue.silver.factstream",
        "alias":"factstream",
        "cols":"factstream.stream_id,factstream.listen_duration"
    },
    {
        "table":"spotify_catalogue.silver.dimuser",
        "alias":"dimuser",
        "cols":"dimuser.user_id,dimuser.user_name",
        "condition":"factstream.user_id = dimuser.user_id"
    },
    {
        "table":"spotify_catalogue.silver.dimtrack",
        "alias":"dimtrack",
        "cols":"dimtrack.track_id,dimtrack.track_name",
        "condition":"factstream.track_id = dimtrack.track_id"
    },
]

# COMMAND ----------

query_txt ="""

    SELECT {% for param in parameters%}
        {{param.cols}}
            {%if not loop.last%},{%endif%}
        {%endfor%}

        FROM

        {%for param in parameters%}
            {%if loop.first%}
                {{param.table}}
            {%endif%}
        {%endfor%}
        {%for param in parameters%}
            {%if not loop.first%}
            LEFT JOIN
            {{param.table}}
            AS
            {{param.alias}} ON
            {{param.condition}}
            {%endif%}
        {%endfor%}



"""

# COMMAND ----------

jinja_sql_str = Template(query_txt)
query=jinja_sql_str.render(parameters=parameters)
print(query)

# COMMAND ----------

display(spark.sql(query))