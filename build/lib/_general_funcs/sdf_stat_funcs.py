from pyspark.sql import functions as F
from pyspark.sql.window import Window

def sdf_normalize_measure(sdf, measure, drop_interim_cols=True, **kwargs):
    """
    Function sdf_normalize_measure to take in sdf and create normalized measures by specified partition cols (if any),
        to normalize measures to 0-1 scale
    params:
        sdf: spark df
        measure str: name of measure column
        drop_interim_cols bool: optional param to specify to drop interim cols and only keep final norm value, default = True
        **kwargs: optional kwargs to pass in:
            partition_cols, list of col(s) if want to normalized across partitions
        
    returns:
        sdf with normalized centrality measure, name of final measure = {measure}_norm
    
    """
    
    # create window to calculate min/max values on (will do full df if no partition columns specified)
    
    norm_window = Window.partitionBy(*kwargs.get('partition_cols',''))
    
    sdf2 =  sdf.withColumn(f"{measure}_log", \
                          F.log10(F.col(measure))) \
              .withColumn(f"{measure}_log_min", \
                          F.min(F.col(f"{measure}_log")).over(norm_window)) \
              .withColumn(f"{measure}_norm0", \
                          F.col(f"{measure}_log") + F.abs(F.col(f"{measure}_log_min"))) \
              .withColumn(f"{measure}_norm0_max", \
                      F.max(F.col(f"{measure}_norm0")).over(norm_window)) \
              .withColumn(f"{measure}_norm0_min", \
                      F.min(F.col(f"{measure}_norm0")).over(norm_window)) \
              .withColumn(f"{measure}_norm", \
                      F.coalesce((F.col(f"{measure}_norm0") - F.col(f"{measure}_norm0_min")) / (F.col(f"{measure}_norm0_max") - F.col(f"{measure}_norm0_min")), F.lit(0)))
    
    # identify and drop newly added interim cols if requested 
    
    if drop_interim_cols:
        interim_cols = list(filter(lambda x: x not in (sdf.columns + [f"{measure}_norm"]), sdf2.columns))
        
        sdf2 = sdf2.drop(*interim_cols)
        
    return sdf2