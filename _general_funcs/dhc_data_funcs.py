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

        select a.*
               ,b.hq_city as defhc_city
               ,b.hq_state as defhc_state
               ,b.firm_type as defhc_type
               ,c.primaryspecialty
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

       left join  
       definitivehc.hospital_all_companies b
       on a.defhc_id = b.hospital_id

       left join
       martdim.d_provider c
       on a.physician_npi = c.npi

       where rn=1
           
           
   """)
