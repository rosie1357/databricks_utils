# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **base_python_funcs.py: This notebook contains any functions related to base python (eg dealing with lists, stringsm date)**

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

def unique_list_elements(list1, list2):
    """
    Function unique_list_elements to identify elements in one list and not the other
    params:
        list1 list: base list
        list2 list: compare list, will return list of elements on THIS list and not base list
        
    returns: list
    
    """
    
    return list(set(list2).difference(list1))

# COMMAND ----------

def unique_items_part(items, num, sep='/'):
    """
      Function unique_items_part to split input list of items on given separator, take specified split number, 
          and return unique set of strings

      params:
          items list: list of strings to split and pull from, eg ['carlos/boy', 'gigi/girl', 'lulu/girl']
          num int: 0-based index of # to pull from split, eg 1
          sep str: optional separator to split on, default = '/'

      returns:
          sorted list of unique items pulled, eg with examples above, return would be ['boy', 'girl']
  
    """
    return sorted(set(map(lambda x: x.split(sep)[num], items)))

# COMMAND ----------

def add_time(start_date, string_format="%Y-%m-%d", add_days=0, add_weeks=0):
    """
    Function add_time to take in start date as string, add a given time interval,
      and return string date with given time added
      (note only days and weeks options are built in, can add more if needed)
      
    params:
        start_date str: starting date as string
        add_value int: integer value to add
        string_format str: format of date string, default = "%Y-%m-%d" (return will be same type)
        add_interval str: interval to add, default = 'days'
        
    returns:
        string date with string_format
    
    """
    
    new_date = datetime.strptime(start_date, string_format) + timedelta(days=add_days, weeks=add_weeks)
    return new_date.strftime(string_format)
