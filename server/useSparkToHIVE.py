"""




"""

import sys
import json

target_table = sys.argv[1]
data = sys.argv[2]
data = json.loads(data)
# print(type(target_table), '  tohive   ', target_table)
# print(type(data), '  tohive   ', data)

from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName(f"test{target_table}")
         .enableHiveSupport().getOrCreate())
spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
spark.sql("set hive.exec.dynamic.partition = true")

df = spark.createDataFrame(data)
df.show()
# df.write.format("hive").mode('overwrite').insertInto(target_table)
