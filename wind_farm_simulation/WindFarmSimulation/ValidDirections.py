# Databricks notebook source
import dlt
import pyspark.sql.functions as f

@dlt.table()
def total_power():
  total_p = (
    dlt.read_stream("mill_weather")
    .selectExpr("id","day", "generation_time","simulation","""aggregate(
      (
        transform(
          simulation,
          x -> case
            when case
              when abs(wind_bearing - x :: double) <= 180 then abs(wind_bearing - x :: double)
              else 360 - (abs(wind_bearing - x :: double))
            end <= 10 then wind_speed
            else 0 :: double
          end
        )
      ),
      0 :: double,
      (acc, x) -> acc + x :: double
    ) as total_power""")
    )

  return total_p  



# COMMAND ----------

@dlt.table()
def valid_sims():
  too_low = (
    dlt.read("total_power")
    .groupBy("id","day")
    .agg(f.sum("total_power").alias("total_power"))
    .where("total_power < 1000")
    )
  valid_config = (
    dlt.read_stream("total_power")
    .withWatermark("generation_time","10 seconds").dropDuplicates(["id"])
    .join(too_low,["id"], how="leftanti")
    )
  return valid_config

# COMMAND ----------


