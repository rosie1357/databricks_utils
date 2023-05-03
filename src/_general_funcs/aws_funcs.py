import boto3
import json
import os

from shutil import copyfile
from datetime import date
from tempfile import NamedTemporaryFile

from _general_funcs.utils import get_dbutils

TODAY = date.today().strftime("%Y%m%d")

def boto3_s3_client(**cred_kwargs):
    """
    Function boto3_s3_client to return boto3 s3 client
    params:
        **cred_kwargs with given values:
            aws_access_key_id str: aws_access_key_id value
            aws_secret_access_key str: aws_secret_access_key value
            aws_session_token str: OPTIONAL param if mfa needed for aws_session_token value
    """
    
    return boto3.client(
        's3',
        **cred_kwargs
    )

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

def download_save_s3(client, bucket, blob_prefix, key_prefix, **kwargs):
    """
    Function download_save_s3 to download all matching files with a specified prefix from s3 bucket, 
        and save to specified container in blob storage
        will get list of ALL files from s3 bucket for possible matches if s3_files not given in kwargs
        
    params:
        client: boto3 s3 client
        bucket str: name of bucket to pull from
        blob_prefix str: blob container prefix to write to, eg '/dbfs/mnt/datascience/Monocl_Raw_Data'
        key_prefix str: matching key prefix on s3 files to get and write out, eg '2022-11-14/MonoclExperts'
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
    
    get_dbutils().fs.mkdirs(f"{blob_prefix.replace('/dbfs','')}/{key_prefix}")
    
    for file in list(filter(lambda x: x.startswith(f"{key_prefix}"), s3_files)):
        
        # create set of params to pass to single download
        
        params_pass = {'client': client,
                      'bucket': bucket,
                      'file': file,
                      'outfile': f"{blob_prefix}/{file}"}
    
        s3_single_download(**params_pass)

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

def upload_s3(client, bucket, key_prefix, file_path, object_name=None):
    """
    Function upload_s3 to upload file at given file_path to s3 with same file name, to specified bucket/key_prefix
    params:
        client: boto3 s3 client
        bucket str: name of bucket to pull from
        key_prefix: matching key prefix on s3 path to upload to
        file_path str: full file path of file to upload
        object_name str: optional param if want to name uploaded object with different name from input file, otherwise will be name of input file
        
    """
    
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    client.upload_file(file_path, bucket, f"{key_prefix}/{object_name}")
