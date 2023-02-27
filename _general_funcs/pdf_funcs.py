# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **pdf_funcs.py: This notebook contains short functions that act on pandas dataframes**

# COMMAND ----------

import math

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

# COMMAND ----------

def create_ptiles(df, col, ptiles, fillna=True, suffix='ptile', return_zero_null=True, **kwargs):
    """
    Function create_ptiles to read in given df and create ptiles
    params:
        df: pandas df
        ptiles int: # of buckets to create
        col str: name of column to create ptiles from
        fillna bool: optional param to specify whether to fill col with 0s if na, default = True
        suffix str: optional param to specify suffix to add to col name for new ptile col, default = 'ptile'
        return_zero_null bool: optional param to specify whether to set values of 0s to null instead of allowing to be in first ptile, default=True
        **kwargs:
            sort_cols list: optional param to list additional cols to sort by to deal with possible ties
            outcol str: optional param to specify name of output column, otherwise = {col}_{suffix}
        
    returns:
        df with cumpct col and ptiles col
    
    """
    
    # create additional vars
    
    sort_cols = [col] + kwargs.get('sort_cols', [])
    
    outcol = kwargs.get('outcol', f"{col}_{suffix}")
    
    div = 100/ptiles
    
    # fill with 0s if requested
    
    if fillna:
        df[col].fillna(0, inplace=True)
        
    # sort and create % and ptile - if sum total of column = 0, set to 1 to avoid nan error (all ptiles will == 1 as dummy values)
    
    df_sorted = df.sort_values(sort_cols)[col]
    
    tot = max(df_sorted.sum(), 1)
    
    df[f"{col}_cumpct"] = 100*(df_sorted.cumsum() / tot)
    
    df[outcol] = df[f"{col}_cumpct"].apply(lambda x: min(1 + math.ceil(x//div), ptiles) if x == x else np.nan)
    
    if return_zero_null:
        df[outcol] = df[[outcol, col]].apply(lambda x: x[0] if (x[1] > 0) else np.nan, axis=1)
    
    return df
