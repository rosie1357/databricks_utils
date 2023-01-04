# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **aws_funcs.py: This notebook contains any functions related to aws**

# COMMAND ----------

import boto3
import json

from shutil import copyfile
from datetime import date
from tempfile import NamedTemporaryFile
from retry import retry

TODAY = date.today().strftime("%Y%m%d")

# COMMAND ----------

def boto3_s3_client(access_key, secret_key):
    """
    Function boto3_s3_client to return boto3 s3 client
    params:
        access_key str: aws_access_key_id value
        secret_key str: aws_secret_access_key value
    """
    
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

# COMMAND ----------

def list_keys_s3(client, **object_kwargs):
    """
    Function list_keys_s3 to take in client and bucket with optional prefix, 
        and return list of all keys in bucket/prefix
        
    params:
        client: boto3 s3 client
        **object_kwargs: kwargs to pass to paginator.paginate, must include Bucket with optional Prefix
            eg Bucket = MY_BUCKET, Prefix = MY_PREFIX
            
    returns:
        list of all keys in bucket
    
    """
    
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(**object_kwargs)

    return [obj['Key'] for page in page_iterator for obj in page['Contents']]

# COMMAND ----------

def download_save_s3(client, bucket, blob_prefix, key_prefix, val_parquet=False, **kwargs):
    """
    Function download_save_s3 to download all matching files with a specified prefix from s3 bucket, 
        and save to specified container in blob storage
        will get list of ALL files from s3 bucket for possible matches if s3_files not given in kwargs
        
    params:
        client: boto3 s3 client
        bucket str: name of bucket to pull from
        blob_prefix str: blob container prefix to write to, eg '/dbfs/mnt/datascience/Monocl_Raw_Data'
        key_prefix str: matching key prefix on s3 files to get and write out, eg '2022-11-14/MonoclExperts'
        val_parquet bool: optional boolean to validate structure of parquet, will continue max of 5 times until found to be a valid structure
            (can sometimes download corrupted/partial file with missing magic number footers, but on subsequent attempt(s) will read properly),
            default = False (do not validate)
        **kwargs: accepted are s3_files, with list of keys in bucket to pull from, otherwise will go directly to bucket to pull
        
    returns:
        none, saves to blob and prints all files copied
        
    will creatre blob dir if not exists
    
    """
    
    if 's3_files' in kwargs:
        s3_files = kwargs['s3_files']
    
    else:
        s3_files = list_keys_s3(client, Bucket=bucket)
          
    print('\t' + f"Saving raw files to: {blob_prefix}/{key_prefix}")
    
    dbutils.fs.mkdirs(f"{blob_prefix.replace('/dbfs','')}/{key_prefix}")
    
    for file in list(filter(lambda x: x.startswith(f"{key_prefix}"), s3_files)):
        
        # create set of params to pass to either regular single download OR parquet download with validation
        
        params_pass = {'client': client,
                      'bucket': bucket,
                      'file': file,
                      'outfile': f"{blob_prefix}/{file}"}
    
        if (val_parquet) & (file.endswith('parquet')):
            s3_single_download_par_wrapped(**params_pass)
            
        else:
            s3_single_download(**params_pass)

# COMMAND ----------

def s3_single_download(client, bucket, file, outfile):
    """
    Function s3_single_download to download given SINGLE file from s3 to temp file, and copy to perm outfile location
    params:
        client: boto3 s3 client
        bucket str: name of bucket to pull from
        file str: FULL file (key) name of file to download from bucket
        outfile str: name of output file to save as
        
    returns:
        none, copied file to perm location
    """
    
    print('\t\t' + f"-- copying: {file}")
    
    with NamedTemporaryFile(mode='w+b') as f:
        client.download_fileobj(bucket, file, f)
        copyfile(f.name, outfile)

# COMMAND ----------

@retry(tries=5, delay=2)
def s3_single_download_par_wrapped(client, bucket, file, outfile):
    """
    Function s3_single_download_par_wrapped to download given SINGLE PARQUET file from s3 to temp file, and copy to perm outfile location
         will attempt to read temp parquet file max of 5 times with 2 second delay
         (can sometimes download corrupted/partial file with missing magic number footers, but on subsequent attempt(s) will read properly)
         
    params:
        client: boto3 s3 client
        bucket str: name of bucket to pull from
        file str: FULL file (key) name of file to download from bucket
        outfile str: name of output file to save as
    
   returns:
        none, copied file to perm location
    """
    
    print('\t\t' + f"-- copying parquet: {file}")
    
    with NamedTemporaryFile(mode='w+b') as f:
        client.download_fileobj(bucket, file, f)
        copyfile(f.name, outfile)
        spark.read.parquet(outfile.replace('/dbfs',''))

# COMMAND ----------

def read_s3_to_json(client, bucket, key):
    """
    Function read_s3_to_json to read text file from s3 as json and return as dict
    params:
        client: boto3 s3 client
        bucket str: name of bucket to pull from
        key_prefix str: matching key prefix on s3 files to get and write out, eg '2022-11-14/MonoclExperts'
        
    returns: dict of raw text file
    
    """
    
    response = client.get_object(Bucket=bucket, Key = key)

    return json.loads(response['Body'].read().decode('utf-8'))
