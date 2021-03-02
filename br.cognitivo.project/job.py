import os
# SUBMIT_ARGS = "--packages databricks:spark-deep-learning:1.0.0-spark2.3-s_2.11 pyspark-shell"
# os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
import json
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, desc, col, trim, lower

#Default configs
spark = SparkSession.builder.appName("COGNITIVO_ANALISE").getOrCreate()
sc = spark.sparkContext

# Mapper - Data Types
with open("/home/vitorsampaio/IdeaProjects/cognitivoProjectPython/br.cognitivo.project/config/types_mapping.json", 'r') as json_config_file:
    mapper_columns = json.load(json_config_file)

# Raw Data
load_raw = spark.read. \
    option("header", True). \
    csv("/home/vitorsampaio/IdeaProjects/cognitivoProjectPython/br.cognitivo.project/data/input/users")

# Rank
load_rank = load_raw.withColumn("rank", row_number().over(Window.partitionBy("id").orderBy(desc("update_date")))). \
    filter(col("rank") == "1"). \
    sort(desc("id")). \
    drop(col("rank"))

# Normalize
for campo, type in mapper_columns.items():
    load_final = load_rank.withColumn(campo, trim(lower(load_rank[campo].cast(type))))

# Write
load_final. \
    coalesce(1). \
    write. \
    mode("overwrite"). \
    parquet("/home/vitorsampaio/IdeaProjects/cognitivoProjectPython/br.cognitivo.project/data/output/persist")