from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from _general_funcs.sdf_funcs import sdf_add_cond_col

def lev_match_array(sdf, base_name, array_match_names, remove_spaces=True, match_thresholds=[0.3], keep_one=True, addtl_cols_keep=[]):
    """
    Function lev_match_array to take in sdf with one base name and array of names to match with to calculate lev distance
    params:
        base_name str: name of base name to calculate lev to match against
        array_match_names str: name of ARRAY type column with names to loop over and match base with
        
        remove_spaces bool: optional boolean to specify whether to remove spaces from all names before performing matching, default=True
                     NOTE! this will NOT remove spaces on the output columns. it will only remove them to perform matching.
        
        match_thresholds list: optional numeric values to use to consider a "match", will create indicators for each threshold in list, eg
                               if match_thresholds = [0.05, 0.1, 0.15], there will be three output 0/1 columns = lev_match_005, lev_match_01, lev_match_015
                               default = [0.3]
                               
        keep_one bool: optional param to specify whether to keep just FIRST best name match, if multiples, default=True
                       if set to False, will return array with 1 or with all best matches if > 1
                       
        addtl_cols_keep list: optional param to specify additional newly created columns to keep
            by default, will only keep: best_lev (float lowest normalized lev value)
                                        best_match_names (string with best name if keep_one==True, otherwise array of name(s) corresponding to best_lev) 
                                        lev_match_### columns (0/1 indicating best_lev <= match_thresholds)
                                        
            other options are: match_names_prep (array of array_match_names with spaces removed if specified
                               base_name_prep (string with base name with spaces removed if specified)
                               lev (array of lev distances)
                               length (array of min length between base and given array name)
                               lev_map (struc array with params array_match_names, lev, and length)
                               lev_norm (array of lev / length)
            
        
    returns:
        sdf with cols specified above
    
    """
    
    # list of columns to keep - will add each lev_match col when created below
    
    COLS_KEEP = sdf.columns + ['best_lev', 'best_match_names'] + addtl_cols_keep
    
    # create additional filter based on keep_one param - filter will take only the first in array of best_matches if true, otherwise take the full array
    
    addtl_filter = F.col('best_matches')[0]
    if keep_one==False:
        addtl_filter = F.col('best_matches')
        
    # create string replace_space_with value to specify what to replace spaces with - if remove_spaces is True, will replace with '' (remove spaces),
    # otherwise will just replace with space so no effect
    # (note this is surely not the best way to do this so should make more efficient in future)
    
    replace_space_with = ''
    if remove_spaces==False:
        replace_space_with = ' '
    
    sdf = sdf.withColumn('match_names', F.col(array_match_names)) \
             .withColumn('match_names_prep', F.transform('match_names', lambda x: F.regexp_replace(x, ' ', replace_space_with))) \
             .withColumn('base_name_prep', F.regexp_replace(F.col(base_name), ' ', replace_space_with)) \
             .withColumn('lev', F.transform('match_names_prep', lambda x: F.levenshtein(x, F.col('base_name_prep')).cast(DoubleType()))) \
             .withColumn('length', F.transform('match_names_prep', lambda x: F.greatest(F.length(x), F.length(F.col('base_name_prep')).cast(DoubleType())))) \
             .withColumn('lev_map', F.arrays_zip(F.col('match_names'), F.col('lev'), F.col('length'))) \
             .withColumn('lev_norm', F.transform('lev_map', lambda x: x.lev / x.length)) \
             .withColumn('best_lev', F.array_min('lev_norm')) \
             .withColumn('best_matches', F.filter('lev_map', lambda x: x.lev / x.length == F.col('best_lev')).match_names) \
             .withColumn('best_match_names', addtl_filter)
    
    # loop over thresholds passed in match_thresholds, create indicator for each
    
    for threshold in match_thresholds:
        
        thresh_col = f"lev_match_{str(threshold).replace('.','')[0:4]}"
        COLS_KEEP = COLS_KEEP + [thresh_col]
    
        sdf = sdf_add_cond_col(sdf = sdf,
                               newcol = thresh_col,
                               condition = F.col('best_lev')<=threshold) 

    return sdf.select(*COLS_KEEP)

def match_string_contains(incol, matchcol):
    """
    Function match_string_contains to return expression to create boolean if matchcol is contained within incol,
        accounting for spaces before and after, OR beginning of line instead of first space, OR end of line instead of last space,
        e.g. to AVOID matching if matchcol value='LEE' and incol value='SELEENA'
        
    params:
        incol col: string column on pyspark df
        matchcol col: string column on pyspark df
        
    returns:
        F.expr described above to return boolean
    
    """
    
    return F.expr(f"{incol} rlike concat('([^A-Za-z]|^)', {matchcol}, '([^A-Za-z]|$)')")