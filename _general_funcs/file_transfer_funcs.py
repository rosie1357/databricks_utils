from shutil import copyfile
from datetime import date

TODAY = date.today().strftime("%Y%m%d")

def to_csv(sdf, outname, outdir="/tmp", array_cols=[]):
    """
    Function to_csv to save sdf as csv to be downloaded
    params:
        sdf: spark df 
        outname str: name of output csv (without .csv extension)
        outdir str: optional path to save, default = /tmp
        array_cols list: optional param, if any cols are array must convert to string
        
    returns:
        string name of output csv, prints path file saved to
    """
        
    outpath = f"{outdir}/{outname}.csv"
    
    # if any array columns, must convert to string concatenated by comma (csv does not support array)

    for col in array_cols:
        
        sdf = sdf.withColumn(col, F.array_join(F.col(col), ', '))
        
    # must delete if exists
    try:
        dbutils.fs.ls(outpath)
        dbutils.fs.rm(outpath, True)
    except:
        pass
    
    sdf.toPandas().to_csv(outpath, index=False)
    
    print(f"File saved to {outpath}")
    
    return outpath

def save_download(filename, outdir, currentdir='/tmp', host_auth='https://adb-41629281235515.15.azuredatabricks.net', add_date=True, date=TODAY):
    """
    Function save_download to save a file to location in dbfs, and then return the location to download the saved file
    params:
        filename str: name of file to save
        outdir str: directory on dbs to save file
        currentdir str: directory of file CURRENT location, default = /tmp (because /tmp is local, can save full file there and then copy directly - dbfs does not support random writes or direct appends)
        host_auth str: https address for host url and authentication number for given setup, default is above
        add_date bool: optional param to specify whether to add date to end of file name, default=True
        
    NOTE!! This will only work with a file to be saved in /dbfs/FileStore/  
        
    returns:
        print with url to copy into browser to automatically download file
    """
    
    dbfs_path = '/dbfs/FileStore'
    
    assert outdir.startswith(dbfs_path), f"ERROR: outdir must starts with {dbfs_path} for this function to work - FIX"
    
    filename_out = filename
    if add_date:
        filename_out = f"{filename_out.split('.')[0]}_{date}.{filename_out.split('.')[1]}"

    copyfile(f"{currentdir}/{filename}", f"{outdir}/{filename_out}")

    print(f"Download file here: {host_auth}/{outdir.replace(dbfs_path,'files')}/{filename_out}")
