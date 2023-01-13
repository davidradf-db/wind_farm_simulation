# Databricks notebook source
# MAGIC %pip install shapely

# COMMAND ----------

import random
import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import math
import numpy as np
from shapely.geometry import Polygon
from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, struct
import json

# 4 square miles
lat_upper = 40.1
lat_lower = 40.13
lng_upper = -90.1
lng_lower = -90.13

safe_distance_meters = 100
num_windmills = 500

@udf("array<struct<lat:double,lng:double,  mill_id:int>>")
def generate_random_lat_long(lat_upper, lat_lower, lng_upper, lng_lower):
    points = []
    for i in range(num_windmills):
        lat = random.uniform(lat_upper, lat_lower)
        lng = random.uniform(lng_upper, lng_lower)
        points.append({'lat':lat, 'lng':lng, "mill_id":i})
    return points

def azimuth_offset(direction, offset) -> int:
  if abs(direction - offset) <= 180:
    return abs(direction - offset)
  else:
    return 360 - (360 - (abs(offset - direction)))

#https://stackoverflow.com/questions/5182816/given-point-lat-and-long-find-coordinates-of-corners-of-a-square-with-given-d
def calc_lat_long(line_dir, center_lat, center_long, distance):
  R = 6371000
  brng = line_dir * ( math.pi / 180 )
  lat = math.asin( math.sin( center_lat ) * math.cos( distance / R ) + math.cos( center_lat ) * math.sin( distance / R ) * math.cos( brng ) )
  lng = center_long + math.atan2( math.sin( brng ) * math.sin( distance / R ) * math.cos( center_lat ), math.cos( distance / R ) - math.sin( center_lat ) * math.sin( lat ) )
  return lat *(180/math.pi),lng *(180/math.pi)


@udf("struct<ur:struct<lat:double,lng:double>, ul:struct<lat:double,lng:double>, bl:struct<lat:double,lng:double>, br:struct<lat:double,lng:double>, c_lat:double, c_lng:double>")
def build_square(center_lat, center_long, distance, mill_dir):

  _center_lat = center_lat * ( math.pi / 180 ) # Only do this if you need to convert from deg. to dec.
  _center_long = center_long * ( math.pi / 180 ) # Only do this if you need to convert from deg. to dec.

  corner_list = []

  lat, lng = calc_lat_long(azimuth_offset(mill_dir, 45), _center_lat, _center_long, distance)
  corner_list.append({"lat":lat,"lng":lng})

  lat, lng = calc_lat_long(azimuth_offset(mill_dir, 135), _center_lat, _center_long, distance)
  corner_list.append({"lat":lat,"lng":lng})

  lat, lng = calc_lat_long(azimuth_offset(mill_dir, 225), _center_lat, _center_long, distance)
  corner_list.append({"lat":lat,"lng":lng})

  lat, lng = calc_lat_long(azimuth_offset(mill_dir, 315), _center_lat, _center_long, distance)
  corner_list.append({"lat":lat,"lng":lng})

  mlat = sum(x['lat'] for x in corner_list) / len(corner_list)
  mlng = sum(x['lng'] for x in corner_list)/ len(corner_list)

  def algo(x):
    return (math.atan2(x['lat'] - mlat, x['lng'] - mlng) + 2 * math.pi) % (2*math.pi)

  sorted_corners = sorted(corner_list,key=algo) 



  

  return {"ur":sorted_corners[0], "ul":sorted_corners[1], "bl":sorted_corners[2], "br":sorted_corners[3], "c_lat":center_lat, "c_lng":center_long}
  
def som(rect_json):
  rect = json.loads(rect_json)
  rect1 = rect['rect1']
  rect2 = rect['rect2']

  p1 = Polygon([(rect1['ur']['lat'], rect1['ur']['lng']), (rect1['ul']['lat'], rect1['ul']['lng']),(rect1['bl']['lat'], rect1['bl']['lng']),(rect1['br']['lat'], rect1['br']['lng'])])
  p2 = Polygon([(rect2['ur']['lat'], rect2['ur']['lng']), (rect2['ul']['lat'], rect2['ul']['lng']),(rect2['bl']['lat'], rect2['bl']['lng']),(rect2['br']['lat'], rect2['br']['lng'])])
  return p1.intersects(p2)

#def overlap2(r1_ur: pd.Series, r1_ul: pd.Series, r1_bl: pd.Series, r1_br: pd.Series, r2_ur: pd.Series, r2_ul: pd.Series, r2_bl: pd.Series, r2_br: pd.Series) -> pd.Series:
#def overlap2(r1: pd.Series, r2: pd.Series) -> pd.Series:
@pandas_udf("boolean")
def overlap2(rect_json: pd.Series) -> pd.Series:
  return rect_json.apply(lambda x: som(x))


# COMMAND ----------

@dlt.table(
  spark_conf={"pipelines.trigger.interval" : "30 seconds"},
  schema = "id bigint generated always as identity,  generation_time timestamp, lat_lng_simulation array<struct<lat:double,lng:double, mill_id:int>>"
)
def mill_place():
  
  return (
  spark.readStream.format("rate-micro-batch")
  .option("rowsPerBatch", 10)
  .load()
  # .join(valid_sims)
  .withColumn("generation_time",F.current_timestamp())
  .withColumn("lat_lng_simulation",generate_random_lat_long(F.lit(lat_upper),F.lit(lat_lower),F.lit(lng_upper),F.lit(lng_lower)))
  .drop("timestamp")
  .drop('value')
  )
  
  


# COMMAND ----------

# @dlt.table()
# def mill_dir_place():
#   mill_place = dlt.read("mill_place")
#   # valid_sims = dlt.read("valid_sims").alias("valid").join(dlt.read("mill_dirs").alias("all"), "id").selectExpr("valid.id as mill_sim_id","valid.generation_time as mill_sim_generation_time","all.simulation")
#   return(
#     dlt.read_stream("valid_sims").withColumnRenamed("id","mill_sim_id").withColumnRenamed("generation_time", "mill_sim_generation_time").join(mill_place, how="cross")
#   .withColumn("lat_lng_simulation",generate_random_lat_long(F.lit(lat_upper),F.lit(lat_lower),F.lit(lng_upper),F.lit(lng_lower), F.col("simulation")))
#   .drop('simulation')
    
#   )

# COMMAND ----------

@dlt.table()
def mill_rect():
  valid_sims = dlt.read_stream('valid_sims').selectExpr("posexplode(simulation) as (mill_id,dir)", "id as mill_sim_id", "generation_time as mill_sim_generation_time").withWatermark("mill_sim_generation_time", "8 hours")
  return(
    dlt.read_stream("mill_place").withWatermark("generation_time", "8 hours")
    .withColumn("lat_lng_simulation",F.explode("lat_lng_simulation"))
    .withColumn("mill_id", F.col("lat_lng_simulation.mill_id"))
    .join(valid_sims, "mill_id").repartition(200)
    .withColumn("lat_lng_simulation",build_square("lat_lng_simulation.lat", "lat_lng_simulation.lng", F.lit(safe_distance_meters), "dir"))
  )

# COMMAND ----------

@dlt.table()
def overlaps():
  rect = dlt.read_stream("mill_rect").withWatermark("generation_time", "8 hours")
  return(
    dlt.read_stream("mill_rect").withWatermark("generation_time", "8 hours").alias("orig").repartition(200).join(rect.alias("check"), ["id", "mill_sim_id"]).where("orig.mill_id <> check.mill_id").repartition(200)
    .withColumn("intersects", overlap2(F.to_json(F.struct(F.col("orig.lat_lng_simulation").alias("rect1"), F.col("check.lat_lng_simulation").alias("rect2")
    ))))
    .selectExpr("orig.*","intersects").select("id","lat_lng_simulation.c_lat","lat_lng_simulation.c_lng","generation_time", "mill_sim_id", "mill_sim_generation_time", "mill_id","intersects")
    .withWatermark("generation_time","30 seconds").dropDuplicates(["id", "mill_id", "mill_sim_id"])
  )

# COMMAND ----------

@dlt.table()
def valid_mill_placement():
  overlaps = dlt.read("overlaps").where("intersects = true")
  return(
    dlt.read_stream("overlaps").alias("orig").join(overlaps.alias("check"), ['id', "mill_sim_id"], how="leftanti")
    .selectExpr("orig.*")
  )
