from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid
from datetime import datetime

session = SparkSession.builder\
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport()\
    .getOrCreate()
    
logs = session.sql("select * from gudata.fnszags")

schema = StructType([
    StructField("type_az", StringType(), True),
    StructField("num_az", StringType(), True),
    StructField("date", StringType(), True),
    StructField("state", StringType(), True),
    StructField("num_ver_az", StringType(), True),
])

csv = session.read.format("csv")\
.options(header=True, delimiter = ';')\
.schema(schema)\
.load("/user/niips0/data/RAW/gudata/fns")

tmp_dir = '/tmp/gudata/fns/zags/result.avro'

result = logs.join(csv, logs.registry_number == csv.num_az, "outer").where("registry_number is null")
rs = result.select(col("num_az").alias('registry_number'), col("date")).repartition(1)
rs.write.format("avro").save(tmp_dir)

csv_out = session.read.format("avro").load(tmp_dir)

rs.write.options(header=True, delimiter = ';').csv("/tmp/gudata/result.csv")

partition_key = datetime.now().strftime('%Y-%m-%d')

csv\
  .withColumn('export_date', lit(partition_key))\
  .write.format("hive").partitionBy('export_date').mode("append")\
  .saveAsTable("gudata.zags")
