import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations

def pairwise_combos(array_col):
    """
    Function pairwise_combos to take in array_col and return all pairwise combinations as list of lists
        will return list of one-element list if only one value in input array
        
        to be converted into udf
        
    params:
        array_col str: name of input array column
        
    returns:
        list of lists
    """
    
    if len(array_col)==1:
        return list(array_col)
    
    return list(combinations(array_col,2))

pairwise_combos_udf = F.udf(lambda x: pairwise_combos(x),
                            returnType=ArrayType(ArrayType(StringType()))
                           )