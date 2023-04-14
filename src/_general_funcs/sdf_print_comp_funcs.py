import datacompy
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def sdf_check_distinct(sdf, cols):
    """
    Function to check given sdf is distinct by given set of cols
    params:
        sdf: spark dataframe
        cols list: list of cols to check distinct by
        
    returns:
        print of the following:
            - count of total records
            - count of unique values by given cols
            - distinct/not distinct
    """
    
    grouped = sdf.groupBy(*cols) \
                 .count()
    
    tot_recs, unique_recs = grouped.agg(F.sum('count'), F.count('count')).collect()[0]
    
    result = 'RESULT: Distinct'
    if tot_recs != unique_recs:
        result = result.replace('Distinct', 'NOT Distinct')
    
    return f"Total records: {tot_recs}, Unique records: {unique_recs}, {result}"  

def sdf_frequency(sdf, cols, order='count', maxobs=25, with_pct=False, weight=None, return_df=False, to_print=True):
    """
    Function to get freq of cols on input spark df and return df
    params:
        sdf: input spark df
        cols list: list of cols to get counts by, eg ['col1','col2']
        order str: optional param to specify order of output, default = 'count' (will order by descending count),
                   if set to 'cols', will order by freq column values
        maxobs int: optional param to specify max obs to show, default=25
        with_pct bool: optional param to specify whether to include pcts (will slow performance), default=False
        weighted str: optional value to specify string name of col to use as weight to calculate counts and pcts, default= None
        return_df bool: optional param to specify whether to return a df with freq in addition to printing it, default=False
        to_print bool: optional param to specify whether to print the freq, default=True
  
    returns:
        none (prints counts)
  
    """
    
    orderby = F.col('count').desc()
    if order=='cols':
        orderby = cols
        
    if weight is None:

        freq = sdf.groupBy(*cols) \
               .count() \
               .orderBy(orderby)
        
        if with_pct:
            freq = freq.withColumn('percent', F.round((F.col('count')/ sdf.count())*100,2))
            
    elif weight:
        
        tot = sdf.agg(F.sum(weight)).collect()[0][0]
        
        freq = sdf.groupBy(*cols) \
               .agg(F.count(weight).alias('count'), F.sum(weight).alias('count_weighted')) \
               .withColumn('percent', F.round((F.col('count')/ sdf.count())*100,2)) \
               .withColumn('percent_weighted', F.round((F.col('count_weighted') / tot)*100,2)) \
               .orderBy(orderby)
    
    if to_print:
        freq.show(maxobs, truncate=False)
    
    if return_df:
        return freq

def sdfs_compare(base_sdf, comp_sdf, table_name, join_cols, **kwargs):
    """
    Function sdfs_compare to use datacompy to compare two spark dfs and generate report/all differences
    params:
        base_sdf: spark df to use as base
        compare_sdf: spark df to use as comparison
        table_name str: name of tables comparing (just for print of table name before print of comp report)
        join_cols list: cols to join on for comparison
        **kwargs:
            ignore_cols list: optional param to specify cols to drop before comparison
            base_name, comp_name strings: optional params to label base and comp sets, defaults to BASE and COMP
        
    returns:
        tuple of two spark dfs: 
            first has all rows on both dfs with ANY differences in columns
            second has stacked set of rows on only base or only compare, with column 'source' = "{base_name}_ONLY" or "{comp_name}_ONLY"
            
    prints out human-readable comparison report with summary stats
    
    """
    
    # extract kwargs if given 
    
    ignore_cols = kwargs.get('ignore_cols', [])
    base_name, comp_name = kwargs.get('base_name', 'BASE'), kwargs.get('comp_name', 'COMP')
    
    # drop cols to ignore before comparisons
    
    base, comp = base_sdf.drop(*ignore_cols), comp_sdf.drop(*ignore_cols)
    
    # create comparison instance
    
    comparison = datacompy.SparkCompare(spark, 
                                        base, 
                                        comp, 
                                        join_columns=join_cols)
    
    print(f"Comparison of tables: {table_name}")
    print(comparison.report())
    
    # use comparison instance properties to get differences (only on base or compare, on both but with mismatching rows)
    # add base and comparison date columns
    

    mismatches = comparison.rows_both_mismatch.withColumn('base_date', F.lit(base_name)).withColumn('comp_date', F.lit(comp_name))
    
    base_only = comparison.rows_only_base \
                          .withColumn('source', F.lit(f"{base_name}_ONLY"))
    
    compare_only = comparison.rows_only_compare \
                             .withColumn('source', F.lit(f"{comp_name}_ONLY"))
    
    return mismatches, base_only.unionByName(compare_only).withColumn('base_date', F.lit(base_name)).withColumn('comp_date', F.lit(comp_name))
