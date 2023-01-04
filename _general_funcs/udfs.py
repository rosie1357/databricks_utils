# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **udfs.py: This notebook contains any udfs**

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations

# COMMAND ----------

def pairwise_combos(array_col):
    """
    Function pairwise_combos to take in array_col and return all pairwise combinations as list of lists
        will return list of one-element list if only one value in input array
        
        to be converted into udf
        
    params:
        array_col str: name of input array column
        
    returns:
        list of lists
    """
    
    if len(array_col)==1:
        return list(array_col)
    
    return list(combinations(array_col,2))

pairwise_combos_udf = F.udf(lambda x: pairwise_combos(x),
                           returnType=ArrayType(ArrayType(StringType()))
                           )

"""
NOTE! Attempted to use pandas udf (operates on series instead of row) because the above is slow, but was not able to get it to work
due to error on pyarrow conversion (unclear if return type is too complex for pandas udf at this time)

attempted code:

@pandas_udf(returnType=ArrayType(ArrayType(StringType())))
def pairwise_combos(arr_col):
    
    return arr_col.map(lambda x: list(map(list, combinations(x,2))))

"""

# COMMAND ----------


