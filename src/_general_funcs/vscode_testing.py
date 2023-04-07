from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql('select * from monocl_raw.monocl_mesh limit 1').show()
























