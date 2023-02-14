# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **dhc_data_funcs.py: This notebook contains any functions to pull DHC data**

# COMMAND ----------

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

# COMMAND ----------

def create_ind(CodeType, ind, codes, string_values=True, getmax=False):
    """
    Function create_ind to return statement to create indicator based on code sets (based on using table mxmart.f_mxcodes)
    params:
        CodeType str: value for CodeType to condition on, eg 'DIAGNOSIS'
        ind str: name of indicator to create if match, will add _code to the end
        codes list: list of codes to match on
        string_values bool: optional param to specify whether codes are string and to put quotes around each, default = True
        getmax bool: optional param to specify whether the ind will be used in aggregations and should put max() around statement, default = False
        
    returns:
        case statement to create ind
    
    """
    
    if string_values:
        
        codes_str = ', '.join(f"'{c}'" for c in codes)
    else:
        codes_str = ', '.join(f'{c}' for c in codes)
        
    stmt = f"case when upper(CodeType) = '{CodeType}' and CodeValue in ({codes_str}) then 1 else 0 end"
    
    if getmax:
        stmt = f"max({stmt})"
    
    return f"{stmt} as {ind}_code"
