# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **sdf_array_funcs.py: This notebook contains functions that work on sdf arrays**

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def sdf_list_in_array(sdf, array_col, match_list, match_ind='any_match', **kwargs):
    
    """
    Function sdf_list_in_array() to identify whether ANY element of an array column matches ANY element of a list
    params:
        sdf: spark df
        array_col str: name of array column
        match_list list: list to match against
        match_ind str: optional param with name of match indicator, default = 'any_match'
        *kwargs:
            if match_names in kwargs, will create indicators for each value in match_list based on names in match_names
            
    returns:
        sdf with column 'any_match', which = 1 if any match found, 0 otherwise
    
    """
    
    # create array col of 'matched_values', which is ALL values from input array contained in list (originally set to None if value not in list, but then Nones are removed with F.array_except)
    # create indicator match_ind, with = 1 if 'matched_values' has size >0, otherwise = 0
    
    sdf = sdf.withColumn('matched_values', F.array_except(F.transform(array_col, lambda x: F.when(x.isin(match_list), x).otherwise(None)), F.array(F.lit(None)))) \
             .withColumn(match_ind, F.when(F.size(F.col('matched_values'))>0, 1).otherwise(0))

    if 'match_names' in kwargs:
        
        match_names = kwargs['match_names']

        for value, name in zip(match_list, match_names):
            sdf = sdf.withColumn(name, F.array_contains(F.col(array_col), value).cast('integer'))
        
    return sdf

# COMMAND ----------

def sdf_array_overlap_rm(sdf, base_col, comp_cols):
    """
    Function sdf_array_overlap_rm to remove any overlap from comp_cols that exists between base_col and each comp_col
    params:
        sdf: spark df
        base_col str: name of base array column
        comp_cols list: list of array column(s) to compare base column against and remove any overlap from if exists
        
    returns:
        sdf with same columns as input but with any overlap removed from comp_cols
  
    """
    
    # loop over comp_cols and remove any overlap of base_col and comp_col if exists

    for comp_col in comp_cols:
        
        sdf = sdf.withColumn(comp_col, F.array_except(comp_col, F.array_intersect(F.col(base_col), F.col(comp_col))))
    
    return sdf

# COMMAND ----------

def sdf_extract_struct(sdf, struct_col, struct_field, new_col):
    """
    Function sdf_extract_struct() to extract a specific field from structured array, return an array with those fields extracted, with nulls removed
    params:
        sdf: spark df
        struct_col str: name of structured array column
        struct_field str: name of field to extract from structured array
        new_col str: name of column with extracted array
        
    returns:
        sdf with new_col
    
    """
    
    return sdf.withColumn(new_col, F.array_except(F.col(struct_col).getField(struct_field), F.array(F.lit(None))))

# COMMAND ----------

def sdf_concat_arrays_unq(sdf, cols, new_col, as_size=False):
    """
    Function sdf_concat_arrays_unq to concatenate array columns and create distinct array, optionally returning size instead of array
        will coalesce null arrays to empty to properly concat
        
    params:
        sdf: spark df
        cols list: list of array cols to concat, eg ['array_col1', 'array_col2']
        new_col str: name of new column to create
        as_size bool: optional param to specify new_col should be size of distinct array instead of array itself, default = false
        
    returns:
        sdf with new_col
    
    """

    sdf = sdf.withColumn(new_col, F.array_distinct(F.concat(*[F.coalesce(col, F.array()) for col in cols])))
    
    if as_size:
        sdf = sdf.withColumn(new_col, F.size(new_col))
        
    return sdf
