# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **pdf_funcs.py: This notebook contains short functions that act on pandas dataframes**

# COMMAND ----------

def frequency(df, cols, drop_null=False):
    """
    Function to get freq of cols on input df and return df
    params:
        df: input pandas df
        cols list: list of cols to get counts by
        drop_null bool: optional param to specify whether to drop na, default=False (include na)
  
    returns:
        pandas df that can be printed with readable counts
  
    """
    
    return df.groupby(cols, dropna=drop_null, as_index=False).size().rename(columns={'size': 'total_count'})

# COMMAND ----------

def split_dataframe(df, n_per_split):
    """
    Function split_dataframe to split into df into equal sized chunks and return list of split dfs
    
    params:
        df: pandas df to split
        n_per_split int: # of rows to put in each split
            
    returns:
        list of dfs
    
    """
        
    return [df[i: i+n_per_split] for i in range(0, df.shape[0], n_per_split)]

# COMMAND ----------

def concatenate_cols(df, cols, sep=' '):
    """
    Function concatenate_cols to concatenate string column values and return joined series
    params:
        df: pandas df
        cols list: list of cols to join, eg ['first_name','last_name'], will fill any null values with ''
        sep str: optional parameter to specify what to use as separator to join with, default = ' '
        
    returns:
        pandas series   
        
    will error if any input cols are non-string
    
    """
    
    return pd.Series(df[cols].fillna('').values.tolist()).map(lambda c: sep.join(c))
    
