# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **sdf_funcs.py: This notebook contains functions that act on/with spark dataframes**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from random import random
from functools import reduce
from operator import add

# COMMAND ----------

# MAGIC %run ./fs_funcs

# COMMAND ----------

def union_to_sdf(tables_list, cols=['*'], col_check=True, union_type='all'):
    """
    Function to union all hive tables specified in tables_list, AFTER checking columns are the same
    on all tables if col_check=True
    
    params:
        tables_list list: list of tables to union, eg ['db.table1','db.table2']
        cols list: optional param to specify cols to read in list form, default is all
              e.g. to read only columns 'name' and 'address', set cols=['name', 'address']
        col_check bool: optional param to specify whether to error if columns are not equal across all tables, default=True
        union_type str: optional param for type of union, default = 'all', other option = 'distinct'
        
        will only perform col_check if cols = default of '*'
  
    returns:
        spark df 
  
    """
    
    assert union_type in ['all','distinct'], f"union_type must = all or distinct, passed value = {union_type} - FIX!"
    
    cols_pull = ', '.join([col for col in cols])
    
    if (col_check) and (cols==['*']):
        # create list of strings with column names in all input tables, confirm equal
        
        cols_list = [hive_tbl_cols(tbl) for tbl in tables_list]
        assert len(set(cols_list))==1, f"ERROR: All columns are not equal on input tables ({tables_list}) - CANNOT UNION"
        
    query = (f" union {union_type} "\
             .join([
                   f"""
                   select {cols_pull}
                   from 
                   {tbl}
                    """ 
                    for tbl in tables_list
                   ])
             );   
    
    return spark.sql(query)

# COMMAND ----------

def sdf_add_cond_col(sdf, newcol, condition, if_true=1, if_false=0):
    """
    Function to create a new column based on a conditional on a spark df
    params:
        sdf: spark dataframe to add new column to
        newcol str: name of new column to add
        condition code: condition to evaluate, eg F.col('column1')>0
        if_true code: what to do if true, eg F.col('column1') - default = 1 (to create 0/1 indicator)
        if_false code/str/num: what to do if false, eg F.col('column2') - default = 1 (to create 0/1 indicator)
        
    returns:
        sdf with new column added
    
    """
    
    return sdf.withColumn(newcol \
                          ,F.when(condition, if_true) \
                          .otherwise(if_false)
                         )

# COMMAND ----------

def add_null_indicator(sdf, col):
    """
    Function add_null_indicator to return sdf with 0/1 indicator if given col is null
    params:
        sdf: spark df
        col str: name of col to evaluate
        
    returns:
        sdf with column added f"{col}_null", = 1 if input col is null, otherwise 0
    
    """
    
    return sdf_add_cond_col(sdf = sdf,
                           newcol = f"{col}_null",
                           condition = F.col(col).isNull()
                           )

# COMMAND ----------

def sdf_rename_cols(sdf, prefix='', suffix=''):
    """
    Function sdf_rename_cols to rename ALL cols on input spark df with prefix and/or suffix
    params:
        sdf: pyspark df
        prefix str: optional prefix value
        suffix str: optional suffix value
        
    returns:
        spark df with cols renamed
    
    """
    
    return sdf.select(*[F.col(c).alias(f"{prefix}{c}{suffix}") for c in sdf.columns])

# COMMAND ----------

def sdf_rename_subset_cols(sdf, rename_cols, target, replace):
    """
    function sdf_rename_subset_cols() to take in spark df and rename all columns in cols list to replace 'target' with 'replace'
    params:
        sdf: spark df
        rename_cols list: list of cols to rename
        target str: string in col name to be replaced
        replace str: string to replace target with
        
    returns:
        sdf with renamed cols
    
    """
    
    # split current cols on sdf into those we will not rename and those we will
    
    KEEP_COLS = list(filter(lambda x: x not in rename_cols, sdf.columns))
    RENAME_COLS = [F.col(x).alias(x.replace(target, replace)) for x in rename_cols]
    
    return sdf.select(*KEEP_COLS + RENAME_COLS)

# COMMAND ----------

def sdf_col_contains(sdf, incol, newcol, string_list):
    """
    Function sdf_col_contains to take in list of strings and create 0/1 indicator if incol contains ANY string in list
    
    """
    
    return sdf_add_cond_col(sdf = sdf
                           ,newcol = newcol
                           ,condition = F.col(incol).rlike("|".join([f"({i})" for i in string_list]))
                           )

# COMMAND ----------

def sdf_create_window(partition, order=None, rows_between=None):
    """
    Function sdf_create_window to return a window specification to perform window func on dataframe
    params:
        partition list: list of col(s) to partion by
        order list: optional param to pass order if needed, default = None
        rows_between tuple/str: optional param to specify range of rows preceding/following frame, eg (-10, 10),
              OR = 'unboundedPreceding' or 'unboundedFollowing' or 'unboundedBoth'
        
    returns: Window specification
    
    """
    
    if rows_between == 'unboundedPreceding':
        rows_between = (Window.unboundedPreceding,0)
        
    elif rows_between == 'unboundedFollowing':
        rows_between = (0,Window.unboundedFollowing)
        
    elif rows_between == 'unboundedBoth':
        rows_between = (Window.unboundedPreceding, Window.unboundedFollowing)
    
    window_spec = Window.partitionBy(*partition)
    
    if order:
        window_spec = window_spec.orderBy(*order)
        
    if rows_between:
        window_spec = window_spec.rowsBetween(*rows_between)
        
    return window_spec

# COMMAND ----------

def remove_non_alphanum(sdf, incols, outcols, replace_with='', keep='both', keep_space=False):
    """
    Function remove_non_alphanum to remove anything from input string column(s) that is non-alphanumeric, and replace with replace_with param
        (options to specify ONLY alpha or ONLY num)
    params:
        sdf: input spark df
        incols list: names of input col(s) to clean, if > 1 in list will match with outcols by position
        outcols list: names of output col(s) to clean, if > 1 in list will match with incols by position
        replace_with str: optional param to give value to replace with, default=''
        keep str: optional param to specify what to KEEP, default = 'both' (keep both alpha and num),
            other options = 'alpha' (will remove nums) or 'num' (will remove all alpha)
        keep_space bool: optional param to specify whether to keep spaces, default=False
            if set to True, will still trim leading and trailing spaces, and compress all multiple spaces to one
        
    returns:
        sdf with outcol added
    
    """
    
    assert keep in ['both', 'alpha', 'num'], f"Invalid value of parameter keep passed ({keep}) - must equal both, alpha or num"
    
    regex = '[^A-Za-z0-9]+'
    if keep == 'alpha':
        regex = regex.replace('A-Za-z','')
    elif keep == 'num':
        regex = regex.replace('0-9','')
        
    if keep_space:
        regex = regex.replace(']', ' ]')
        
    for incol, outcol in zip(incols, outcols):
        
        sdf = sdf.withColumn(outcol, F.regexp_replace(F.trim(F.regexp_replace(incol, regex, replace_with)), ' +',' '))
    
    return sdf

# COMMAND ----------

def sdf_rand_sample(sdf, fraction, seed=random(), with_replace=False, samp_id=None, partition_by=None):
    """
    Function to take a random sample of input df and return sample
    params:
        sdf: spark df to sample
        fraction float between 0.0-1.0: fraction to sample
        seed numeric: optional value to use as seed, if not given will use random seed
        with_replace bool: optional boolean to specify whether to sample with replacement, default=False
        samp_id obj: optional string/numeric/etc to use as lit value for ALL records to set samp_id column
        partition_by str/column obj: optional param to specify column to partition by, default=None
        
    returns:
        sample sdf
        
    """
    
    samp_sdf = sdf.sample(fraction = fraction, 
                          withReplacement = with_replace,
                          seed = seed)

    if samp_id is not None:
        samp_sdf = samp_sdf.withColumn('samp_id', F.lit(samp_id))
        
    if partition_by is not None:
        return samp_sdf.repartition(partition_by)
    
    return samp_sdf

# COMMAND ----------

def create_quarter_col(sdf, date_col, quarter_col='quarter'):
    """
    Function create_quarter_col to transform date_col into quarter (year and quarter) column
    params:
        sdf: input spark df
        date_col str: name of date col
        quarter_col str: optional param to specify name of quarter col, default = 'quarter'
        
    returns:
        input sdf with quarter col added     
    """
    
    return sdf.withColumn(quarter_col, F.concat(F.year(date_col).cast('string'), F.lit('Q'), F.quarter(date_col).cast('string')))    

# COMMAND ----------

def sdf_col_to_list(sdf, col):
    """
    Function sdf_col_to_list to return a column on sdf as python list
    params:
        sdf: spark df
        col str: name of column
        
    returns: python list
    
    """
    
    return list(sdf.select(col).toPandas()[col])

# COMMAND ----------

def sdf_add_columns(sdf, cols, new_col):
    """
    Function add_columns to take in list of numeric columns on sdf and add together 
    params:
        sdf: spark df
        cols list: list of cols to add, eg ['count1', 'count2', 'count3']
        new_col str: name of new column created by adding input cols
        
    returns:
        sdf with new_col added
    """
    
    return sdf.withColumn(new_col, reduce(add, [F.col(col) for col in cols]))

# COMMAND ----------

def sdf_return_row_values(sdf, cols):
    """
    Function sdf_return_row_values to select the first row of input sdf, and return tuple
    of all column values specified in cols list
    
    params:
        sdf: spark df
        cols list: list of cols to pull and return
        
    returns:
        tuple with values for first row of sdf for cols specified, OR just individual value if only one col specified
    
    """
    
    values = sdf.select(cols).collect()[0]
    if len(cols) == 1:
        return values[0]
    
    return tuple(values)
