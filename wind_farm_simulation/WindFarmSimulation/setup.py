# Databricks notebook source
dbutils.widgets.text("DatabaseName",'')
dbutils.widgets.text("DatabricksHost",'xxx.cloud.databricks.com')


# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ${DatabaseName};

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/windfarm/weather")

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm /dbfs/tmp/windfarm/weather/weatherHistory.csv
# MAGIC unzip ../weatherHistory.csv.zip -d /dbfs/tmp/windfarm/weather/

# COMMAND ----------

df = spark.read.csv('/tmp/windfarm/weather/',header=True, inferSchema=True).selectExpr("`Wind Speed (km/h)` as wind_speed_kmh", "`Wind Bearing (degrees)` as wind_bearing","`Formatted Date` as reading_time")
df.write.mode("overwrite").saveAsTable(f"{dbutils.widgets.get('DatabaseName')}.weather_history")

# COMMAND ----------

import os, requests, json

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = dbutils.widgets.get('DatabricksHost')
root_dir = os.getcwd().replace("/Workspace","")

dlt_json = """{
  "name":"wind_farm_simulation_demo",
  "target":"%s",
  "configuration":{
  "config.database":"%s"
  },
  "clusters":[
    {
      "label":"default",
      "autoscale":{
        "min_workers":1,
        "max_workers":10
      }
    }
  ],
  "libraries":[
    {
      "notebook":{
        "path": "%s/MillDirectionGenerator"
      }
    },
    {
      "notebook":{
        "path": "%s/ValidDirections"
      }
    },
    {
      "notebook":{
        "path": "%s/MillPlacementGenerator"
      }
    }
  ],
  "continuous":true,
  "development":true
}
"""%(dbutils.widgets.get('DatabaseName'),dbutils.widgets.get('DatabaseName'),root_dir,root_dir,root_dir)


print(dlt_json)
auth_header = {"Authorization":f"Bearer {token}"}
results = requests.post(f"https://{host}/api/2.0/pipelines", json=json.loads(dlt_json), headers=auth_header)
pipeline_id = results.json()['pipeline_id']

# COMMAND ----------

import time

not_started = True
while not_started:
  time.sleep(10)
  if any(f"{pipeline_id}/" == x.name for x in dbutils.fs.ls(f"/pipelines/")):
    not_started=False
    break

spark.readStream.format("delta").load(f"/pipelines/{pipeline_id}/system/events").createOrReplaceTempView("x")



# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   origin.flow_name,
# MAGIC   from_unixtime(round(unix_timestamp(timestamp) / 300) * 300) as five_interval,
# MAGIC   sum(details :flow_progress.metrics.num_output_rows) as rows
# MAGIC from
# MAGIC   x
# MAGIC where
# MAGIC   level = 'METRICS'
# MAGIC   and event_type = 'flow_progress'
# MAGIC   and origin.flow_name is not null
# MAGIC   and details :flow_progress.metrics.num_output_rows is not null
# MAGIC   and origin.flow_name not in ('total_power', 'mill_rect', 'mill_dir_place', 'overlaps')
# MAGIC group by
# MAGIC   flow_name,
# MAGIC   five_interval
# MAGIC order by
# MAGIC   five_interval desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC --wait to execute until records are shown flowing into valid_mill_placement
# MAGIC use ${DatabaseName};
# MAGIC with total_p as (select sum(total_power) total_power, id from total_power group by id),
# MAGIC valid as (select A.*, B.total_power from valid_mill_placement A join total_p B on A.mill_sim_id=B.id)
# MAGIC select c_lat,c_lng, dir from valid 
# MAGIC join (select mill_sim_id ,id from valid order by total_power desc limit 1) using (mill_sim_id, id)
# MAGIC join mill_rect using (mill_id, id, mill_sim_id)

# COMMAND ----------


