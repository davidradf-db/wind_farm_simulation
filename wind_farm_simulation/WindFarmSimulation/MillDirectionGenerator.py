# Databricks notebook source
import random,dlt
from pyspark.sql.functions import *

@udf("array<int>")
def generate_random_directions(mill_count):
    # Create an empty list to store the random numbers
    random_numbers = []
    
    # Generate 10 random numbers
    for i in range(mill_count):
        # Generate a random number between 0 and 360 in 10 degree increments
        random_number = random.randrange(0, 360, 10)
        
        # Add the random number to the list
        random_numbers.append(random_number)
    
    # Return the list of random numbers
    return random_numbers



# COMMAND ----------



# num_simulations = 1000
num_windmills = 500
weather_db = spark.conf.get("config.database")
@dlt.table(
  spark_conf={"pipelines.trigger.interval" : "5 seconds"},
  schema = "id bigint generated always as identity, simulation array<int>, generation_time timestamp"
)
def mill_dirs():
  return (
  spark.readStream.format("rate-micro-batch")
  .option("rowsPerBatch", 10)
  .load()
  .withColumn("simulation",generate_random_directions(lit(num_windmills)))
  .withColumn("generation_time",current_timestamp())
  .drop("timestamp")
  .drop('value')
  )

@dlt.view()
def mill_weather():
  wind = spark.sql(f"""select
  *
from
  (
    select
      sum(wind_speed_kmh) wind_speed,
      wind_bearing,
      reading_time :: date as day
    from
      {weather_db}.weather_history
    group by
      2,
      3
  )""")
  return (
  dlt.read_stream("mill_dirs")
  .join(wind ,how='cross')
  )

# COMMAND ----------


