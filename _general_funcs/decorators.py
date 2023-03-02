# Databricks notebook source
import time

# COMMAND ----------

def timeit(func):
    """
    timeit() decorator to print total elapsed time after run of given func,
        optional kwargs = 'time' with a default of minutes, other options = 'seconds'
    
    """
    def wrapper_function(*args, **kwargs):
        start = time.perf_counter()
        func(*args,  **kwargs)
        time_seconds = time.perf_counter()-start
        if kwargs['time']=='seconds':
            print('\t' + f"Total time: {int(time_seconds)} seconds")
        else:
            print('\t' + f"Total time: {int(time_seconds//60)} minutes")
    return wrapper_function
