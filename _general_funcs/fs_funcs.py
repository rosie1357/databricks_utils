# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **fs_funcs.py: This notebook contains short functions that interact with the databricks file system (reading from, writing to, creating dfs, etc)**

# COMMAND ----------

import pyspark
from pyspark.sql.functions import lit
from delta.tables import DeltaTable

from time import sleep

# COMMAND ----------

def hive_to_df(tbl, cols=['*'], df_type='spark', rec_limit=None, new_cols={}, subset=''):
    """
    Function hive_to_pandas to read from hive database table and return pandas df
    params:
      tbl str: name of table to read
      cols list: optional param to specify cols to read in list form, default is all
          e.g. to read only columns 'name' and 'address', set cols=['name', 'address']
      df_type string: type of df to return, default = 'pandas', other option = 'spark'
      rec_limit int: optional param to specify # of records to pull, default = None (pull all)
      new_cols dict: optional dictionary to specify new columns to add with the same value for all recs, eg {'source': 'dhc_orgs'}
      subset string: optional param to specify subset to make to table, eg "npi_type='1'"
  
    returns:
        pandas df
  
    """
    
    assert df_type in ['pandas','spark'], f"df_type must = pandas or spark, passed value = {df_type} - FIX!"
    
    cols_pull = ', '.join([col for col in cols])
    limit = ''
    if rec_limit is not None:
        limit = f"limit {rec_limit}"
    
    query = spark.sql(f"""
                       select {cols_pull}
                       from {tbl}
                       {subset}
                       {limit}
                       """)
    
    if df_type=='pandas':
        return query.toPandas().assign(**new_cols)
    
    else:
        for k,v in new_cols.items():
            query = query.withColumn(k, lit(v))
            
        return query

# COMMAND ----------

def pyspark_to_hive(df_ps, out_tbl, out_format='delta', out_mode='overwrite', overwrite_schema='false'):
    """
    Function pyspark_to_hive to save pyspark df to hive and file explorer
    params:
        df_ps pyspark df: pyspark df to save
        out_tbl str: outfile table to save to in form of 'DB.TBLNAME'
        out_format str: optional param to specify format, default = delta
        out_mode str: optional param to specify mode to use in writing, default = overwrite
        overwrite_schema str: optional param to specify whether to overwrite schema (write will fail if table exists but columns don't match exactly), 
                              default='false' (this is the default spark behavior when not specifying)
        
    returns:
        none
    
    """
    
    df_ps.write.format(out_format).mode(out_mode).option("overwriteSchema", overwrite_schema).saveAsTable(out_tbl)

# COMMAND ----------

def copy_hive_table(tbl_orig, tbl_copy, shallow=False, can_replace=False):
    """
    Function to copy hive table to new location
    params:
        tbl_orig str: name of original table to copy
        tbl_copy str: name of destination location to copy to
        shallow bool: optional boolean to specify shallow copy, default = False (creates independent full copy)
        can_replace bool: optional boolean to specify whether should allow replacement, or error if already exists, default = False (error if exists)
        
        
    returns:
        none
    
    """

    DeltaTable.forName(spark, tbl_orig).clone(tbl_copy, isShallow=shallow, replace=can_replace)

# COMMAND ----------

def drop_hive_table(tbl_list, must_exist=True):
    """
    Function to drop all tables in given list, will by default throw error if not exists
    params:
        tbl_list list: list of tables, eg ['db.tbl1', 'db.tbl2']
        must_exist bool: optional param to specify whether to error if not exists, default=True (will throw error if not exists)
    
    returns:
        none
    
    """
    
    exist_logic = '' if must_exist else 'if exists'
    
    for tbl in tbl_list:
        spark.sql(f"drop table {exist_logic} {tbl}")

# COMMAND ----------

def list_db_tables(db, name_like='*'):
    """
    Function to return list of all tables in database, with optional name matching
    params:
        db str: name of db to search
        name_like str: optional param to specify name matching, eg 'mytable_*'
        
    returns:
        list
    
    """
    
    return list(spark.sql(f"""
                          show tables in {db} like '{name_like}'
                          """) \
               .toPandas()['tableName'])

# COMMAND ----------

def hive_tbl_cols(tbl, as_list=False):
    """
    Function hive_tbl_cols to return EITHER a list or a comma-separated string of columns on hive table
    params:
        tbl string: table name, eg 'db.table1'
        as_list bool: optional boolean to specify whether to return as list or comma-separated string, default=False (return as string)
        
    returns:
        comma-separated list of col names or regular list
    
    """
    
    cols = spark.table(tbl).limit(0).columns
    if as_list==False:
        cols = ", ".join(cols)
    
    return cols

# COMMAND ----------

def hive_tbl_count(tbl):
    """
    Function hive_tbl_count to return the count of rows in given table
    params:
        tbl str: table name, eg 'db.table1'
        
    returns:
        int value of row count
    
    """
    
    return spark.sql(f"select count(1) as count from {tbl}").collect()[0]['count']

# COMMAND ----------

def hive_tbl_count_distinct(tbl, col):
    """
    Function hive_tbl_count to return the count of DISTINCT values across given col in given table
    params:
        tbl str: table name, eg 'db.table1'
        col str: name of col to get distinct by, eg 'unique_id'
        
    returns:
        int value of distinct value counts
    
    """
    
    return spark.sql(f"select count(distinct {col}) as count from {tbl}").collect()[0]['count']

# COMMAND ----------

def hive_frequency(tbl, cols, maxobs=50):
    """
    Function hive_frequency to run freq/crosstab on hive table with counts and percents
    params:
        tbl str: table name, eg 'db.table1'
        cols list: list of column(s) to run, eg ['col1'] or ['col1', 'col2']
        maxobs int: optional param to specify the # of recs to show, default=50
        
    returns:
        none, prints frequency
    
    """
    
    cols_query = ', '.join(cols)
    
    spark.sql(f"""
        select {cols_query}
               ,format_number(count, '#,###') as count
               ,format_number(100*(count/tot_count), '#.###') as pct
               ,format_number(tot_count, '#,###') as tot_count
        from (
            select *, sum(count) over(order by count rows between unbounded preceding and unbounded following) as tot_count 
            from 
                (
                select {cols_query}, count(1) as count
                from {tbl}
                group by {cols_query}
                order by {cols_query}
                ) a
            ) b
    
    """).show(maxobs, truncate=False)

# COMMAND ----------

def hive_sample(tbl, maxobs=50):
    """
    Function hive_sample to print sample of records from hive table
    params:
        tbl str: name of table
        maxobs int: optional param to specify # of obs, default=50
        
    returns: none (prints records)
    """
    
    spark.sql(f"select * from {tbl} limit {maxobs}").display()

# COMMAND ----------

def list_lookup(intable, col, subset_col, subset_value, string=True):
    """
    Function list_lookup() to pull all values of col from table with subset_col = subset_value,
        and return string with list separated by given sep
        
    params:
        intable str: name of input table
        col str: name of column to get values of
        subset_col str: name of column to subset on
        subset_value int/str: value to subset on with subset_col
        string bool: optional param to specify lookup values are string and should be separated by comma with added quotes, otherwise false is comma-only (assumes numeric)
    """

    values = spark.sql(f"""select {col}
                           from {intable}
                           where {subset_col} = {subset_value}
                        """).toPandas()
    sep = "', '"
    if string is False:
        sep = ", "

    joined = sep.join(list(values[col]))
    if string:
        return "'" + joined + "'"
    
    return joined

# COMMAND ----------

def rm_checkpoints(checkdir):
    """
    Function rm_checkpoints to be run when want to clear ALL checkpointed files in the given checkpoint directory
    It will issue a prompt to continue to cancel within 10 seconds (would use regular python input() to ask to proceed but does not work in db notebooks)
    It allows a wait time because it will delete the directory and files recursively then recreate it (fastest/easiest way to do it)
    
    params:
        checkdir str: input path to checkpoints directory
        
    returns:
        string message of all done
        
    prompts to cancel within x seconds if unintended
    
    """
    
    x = 10
    
    print(f"User note! ALL files within {checkdir} will be removed - cancel within {x} seconds if unintended!" + '\n')
    sleep(x)
    
    dbutils.fs.rm(checkdir, True)
    dbutils.fs.mkdirs(checkdir)

    print(f"All files removed from checkpoint directory = {checkdir}!")

# COMMAND ----------

def clear_database(database, sleep_time=10):
    """
    Function clear_database to be run when want to clear ALL all tables from a given directory
    It will issue a prompt to continue to cancel within 10 seconds (would use regular python input() to ask to proceed but does not work in db notebooks)
    It allows a wait time because it will delete the directory and files recursively then recreate it (fastest/easiest way to do it)
    
    params:
        checkdir str: input path to checkpoints directory
        sleep_time int: optional param with # of secs to sleep before deleting, default = 10
        
    returns:
        string message of all done
        
    prompts to cancel within sleep_time seconds if unintended
    
    """
    
    
    print(f"User note! ALL tables within {database} will be removed - cancel within {sleep_time} seconds if unintended!" + '\n')
    sleep(sleep_time)
    
    spark.sql(f"drop schema if exists {database} cascade")
    spark.sql(f"create schema {database}")

    print(f"All tables removed from database = {database}!")
