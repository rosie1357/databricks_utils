import pandas as pd
from functools import reduce

from _general_funcs.sdf_print_comp_funcs import sdf_frequency
from _general_funcs.fs_funcs import hive_to_df

def monocl_mesh_counts(sdf, start_year, end_year, year_col='year', mesh_col='mesh', mesh_tbl='monocl_raw.monocl_mesh', **comp_kwargs):
    """
    Function monocl_mesh_counts() to take in sdf and get counts by mesh term by year, joining to mesh terms descriptions
    params:
        sdf spark df: spark df with year and mesh cols
        start_year int: starting year to get counts for
        end_year int: ending year to get counts for
        year_col str: optional param for name of year col, default = 'year'
        mesh_col str: optional param for name of mesh col, default = 'mesh'
        mesh_tbl str: optional param for location of table with mesh descriptions, default = 'monocl_raw.monocl_mesh'
        **comp_kwargs dict: optional kwargs to determine base record to calculate each other's record % volume off of,
            if not given will default to rec with highest overall count, otherwise kwargs must be key: value where key = name of col and value = value to subset on
            
    returns:
        pandas df with one rec per mesh term with yearly and overall count cols, description col, and % volume col
    
    """

    # get by year, then join all together

    mesh_counts_list = []

    for year in range(start_year, end_year+1):

        counts = sdf_frequency(sdf.filter(F.col(year_col)==year), [mesh_col], return_df=True, to_print=False) \
                              .withColumnRenamed('count', f"count_{year}") \
                              .toPandas() \
                              .set_index(mesh_col)

        mesh_counts_list += [counts]
        
    # join all together and sum to get totals, sort by total count

    mesh_counts = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'),
                         mesh_counts_list)

    mesh_counts['count_all'] = mesh_counts.sum(axis=1)
    mesh_counts = mesh_counts.sort_values('count_all', ascending=False).reset_index()
    
    # join to mesh definitions

    mesh_counts = pd.merge(mesh_counts,
                          hive_to_df(mesh_tbl, cols=['meshId', 'name'], df_type='pandas'),
                          left_on=mesh_col,
                          right_on='meshId',
                          how='left')
    
    # create base number for volume comparison, default is just first (highest sorted by count descending)
    # but if comp_kwarg is given, use key/value as column/value to pull
    # (will error if col does not exist in df)
    
    if comp_kwargs:
        col, value = list(mydict.comp_kwargs())[0]
        comp_value = mesh_counts.loc[mesh_counts[col]==value]['count_all']
        
    else:
        comp_value = mesh_counts['count_all'].iloc[0]
    
    # create volume % column, comparing count_all for first record (assume is term of interest) to count_all for given record

    mesh_counts['vol_pct'] = mesh_counts['count_all'] / comp_value
    
    # return final table with cols in desired order
    
    return mesh_counts[[mesh_col, 'name', 'count_all'] + [c for c in mesh_counts.columns if c.startswith('count_2')] + ['vol_pct']]