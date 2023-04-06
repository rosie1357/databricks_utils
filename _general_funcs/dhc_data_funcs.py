from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def get_primary_affiliation(addtl_cols=[]):
    """
    Function get_primary_affiliation to return a spark df with one rec per NPI with primary affiliation
    params:
        addtl_cols list: optional param to specify additional cols to pull from outside query 
            (so either from definitivehc.hospital_all_companies or martdim.d_provider), default = none
            
    returns:
        spark df as described above
    
    """
    
    cols_stmt = ''
    if addtl_cols:
        cols_stmt = ', ' + ', '.join(addtl_cols)
    
    
    return spark.sql(f"""
    
        select c.*
               ,d.hq_city as defhc_city
               ,d.hq_state as defhc_state
               ,d.firm_type as defhc_type
        
        from (
            select coalesce(a.physician_npi, b.npi) as physician_npi
                   ,a.defhc_id
                   ,a.defhc_name
                   ,b.ProviderName
                   ,b.primaryspecialty
                   {cols_stmt}

           from (

                select physician_npi
                       ,defhc_id
                       ,hospital_name as defhc_name
                       ,row_number() over (partition by physician_npi 
                                           order by score_bucket desc, score desc)
                                     as rn

                        from   hcp_affiliations.physician_org_affiliations
                        where  include_flag = 1 and
                               current_flag = 1

                   ) a

           full join
           martdim.d_provider b
           on a.physician_npi = b.npi
           
           where rn=1 or rn is null

           ) c

       left join  
       definitivehc.hospital_all_companies d
       on c.defhc_id = d.hospital_id
           
   """)

def create_ind(col, ind, codes, CodeType=None, string_values=True, getmax=False):
    """
    Function create_ind to return statement to create indicator based on code sets
    params:
        col str: name of column to match against
        ind str: name of indicator to create if match, will add _code to the end
        codes list: list of codes to match on
        CodeType str: optional param to specify value for CodeType to condition on IF USING MXCODES TABLE, eg 'DIAGNOSIS'
        string_values bool: optional param to specify whether codes are string and to put quotes around each, default = True
        getmax bool: optional param to specify whether the ind will be used in aggregations and should put max() around statement, default = False
        
    returns:
        case statement to create ind
    
    """
    
    if string_values:
        
        codes_str = ', '.join(f"'{c}'" for c in codes)
    else:
        codes_str = ', '.join(f'{c}' for c in codes)
        
    types_str = ''
    if CodeType:
        types_str = f"and upper(CodeType) = '{CodeType}'"
        
    stmt = f"case when {col} in ({codes_str}) {types_str} then 1 else 0 end"
    
    if getmax:
        stmt = f"max({stmt})"
    
    return f"{stmt} as {ind}_code"

def pull_ndc_lists(name, compound_list):
    """
    pull_ndc_lists function to return part of case statement (starting only with when assuming they will be combined)
    to combine all variations of words in compound_list to assign to one name, looking at both prop and non-prop names
    
        example output: when upper(NONPROPRIETARYNAME) like ('%OZEMPIC%') or 
                             upper(PROPRIETARYNAME) like ('%OZEMPIC%') or 
                             upper(NONPROPRIETARYNAME) like ('%SEMAGLUTIDE%') or 
                             upper(PROPRIETARYNAME) like ('%SEMAGLUTIDE%') then 'semaglutide'
                             
     params:
         name str: value for final output col
         compound_list list: list of all compound names (will be sandwiched between %) to match on
     
     returns:
         string with statement like above example
    
    """
    
    stmts = ' or '.join(f"upper(NONPROPRIETARYNAME) like ('%{x.upper()}%') or upper(PROPRIETARYNAME) like ('%{x.upper()}%')" for x in compound_list)
    
    return f"when {stmts} then '{name}'"
