# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **_include_all_funcs.py: This notebook runs all individual function notebooks so only this notebook has to be included to get all functions**

# COMMAND ----------

# general settings

spark.conf.set('spark.sql.shuffle.partitions','auto')

# COMMAND ----------

# MAGIC %run ./fs_funcs

# COMMAND ----------

# MAGIC %run ./pdf_funcs

# COMMAND ----------

# MAGIC %run ./sdf_funcs

# COMMAND ----------

# MAGIC %run ./sdf_print_comp_funcs

# COMMAND ----------

# MAGIC %run ./sdf_array_funcs

# COMMAND ----------

# MAGIC %run ./sdf_stat_funcs

# COMMAND ----------

# MAGIC %run ./matching_funcs

# COMMAND ----------

# MAGIC %run ./classes/FinalTablesClass

# COMMAND ----------

# MAGIC %run ./file_transfer_funcs

# COMMAND ----------

# MAGIC %run ./base_python_funcs

# COMMAND ----------

# MAGIC %run ./aws_funcs

# COMMAND ----------

# MAGIC %run ./notebook_pipeline_funcs

# COMMAND ----------

# MAGIC %run ./udfs

# COMMAND ----------

# MAGIC %run ./utils/monocl_utils

# COMMAND ----------

# MAGIC %run ./dhc_data_funcs
