def get_dbutils():
  from pyspark.sql import SparkSession
  spark = SparkSession.getActiveSession()
  from pyspark.dbutils import DBUtils
  return DBUtils(spark)