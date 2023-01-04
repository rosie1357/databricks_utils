# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **FinalTablesClass.py: This notebook contains the FinalTables class**

# COMMAND ----------

class FinalTables(object):
    
    def __init__(self, denoms_df, stratcols=['claim_year'], type_col='recovery_type', count_col='count_claims', denom_count_col='total_claims'):
        """
        FinalTables class to keep running total of all ways claims were recovered and 
        create formatted tables with counts and pcts of methods overall and by year (or other passed column)
        
        params:
            denoms_df df: total counts of claims by stratcols
            
            stratcols list: optional param to specify stratifications to get totals by, default = ['claim_year']
            type_col str: optional param to specify type column to use in looking at recovery by type, default = 'recovery_type'
            count_col str: optional param to specify main counts to use in evaluating recovery, default = 'count_claims'
            denom_count_col str: optional param to specify col with denom counts on denoms_df, default = 'total_claims'
        
        """
        
        self.denoms_df = denoms_df
        self.stratcols = stratcols
        self.type_col = type_col
        self.count_col = count_col
        self.denom_count_col = denom_count_col
        
        # initialize with empty df with just raw counts by recovery type/stratcols
        
        self.groupcols = [self.type_col] + stratcols
        
        self.counts_df = pd.DataFrame(columns=self.groupcols + [self.count_col])
        
        # create dummy final_dfs dictionary to hold stratified and unstratified totals
        
        self.totals = {'stratified': pd.DataFrame(),
                      'unstratified': pd.DataFrame()
                      }
        
    def print_totals(self, strat_type):
        """
        Method to print totals with counts and pcts formatted for easier reading
        params:
            strat_type str: strat type to display, either 'stratified' or 'unstratified'
        """
        
        format_dict = {self.count_col:  '{:,.0f}',
                       self.denom_count_col:  '{:,.0f}',
                       'pct': '{0:,.2f}', 
                       'cum_pct': '{0:,.2f}'
                      }
        
        df_print = self.totals[strat_type]
        
        for col, form in format_dict.items():
            df_print[col] = df_print[col].map(form.format)
        
        print(df_print.to_markdown(index=False))
        
    def unstrat_counts(self, df, bygroup=True, **kwargs):
        """
        Method unstrat_counts to take in df and return summarized counts without stratcols
        
        params:
            df: pandas df with cols specified in groupcol and sumcol
            bygroup bool: boolean to specify whether to get count by group, default=True
            
            **kwargs: can specify optional params 'groupcol' and/or 'sumcol':
                      defaults: groupcol = self.type_col 
                                sumcol = self.count_col
        
        returns:
            pandas df
        
        """
        
        groupcol = kwargs.get('groupcol', self.type_col)
        sumcol = kwargs.get('sumcol', self.count_col)
        
        if bygroup:
        
            return df[[groupcol,sumcol]].groupby([groupcol]).sum(sumcol).reset_index()
        
        else:
            
            return pd.DataFrame(data={sumcol: [df[sumcol].sum()]})
    
    def add_pcts(self, df, groupcol=None):
        """
        Method add_pcts to create pct and cum_pct columns on input df
        
        params:
            pandas df with self.count_col and self.denom_count_col as num and denom, respectively
            groupcol list: optional param to specify col to group cumsum by, default = None
        
        returns:
            df with two new cols
        
        """
        
        df['pct'] = df[[self.count_col, self.denom_count_col]].apply(lambda x: 100*(x[0]/x[1]), axis=1)
        
        if groupcol is not None:
            df['cum_pct'] = df.groupby(groupcol)['pct'].cumsum()   
            
        else:
             df['cum_pct'] = df['pct'].cumsum()   
                
        return df.reset_index(drop=True)
        
    def create_totals(self):
        """
        Method to join current self.counts_df to totals and print total counts and %s by stratcols and overall
        
        """
        
        tots_strat_df = self.counts_df.merge(self.denoms_df, how='outer', on=self.stratcols).sort_values(self.stratcols + [self.type_col])
        
        # get totals unstratified
        
        counts_df2 = self.unstrat_counts(self.counts_df)
        denoms_df2 = self.unstrat_counts(self.denoms_df, bygroup=False, sumcol=self.denom_count_col)
        
        tots_unstrat_df = counts_df2.merge(denoms_df2, how='cross').sort_values(self.type_col)
        
        # put into self.totals dictionary, create percents and running totals
        
        self.totals['stratified'] = self.add_pcts(tots_strat_df, groupcol=self.stratcols)
        self.totals['unstratified'] = self.add_pcts(tots_unstrat_df)
        
        
    def add_counts(self, new_df):
        """
        method add_counts to read in df with needed cols and sum claim counts to then add to self.counts_df
           
        params:
           new_counts_df df: pandas df with the following columns: recovery_type, stratcols count_claims 
           
        returns:
            print of how many claims by recovery type were added to self.counts_df
        
        """
        
        counts_add_df = new_df.groupby(self.groupcols).sum(self.count_col).reset_index()
        
        self.counts_df = pd.concat([self.counts_df, counts_add_df])
        
        added = self.unstrat_counts(counts_add_df)
        
        print("Counts by recovery type added to total table:\n\n" + str(added))
        
        # create/recreate total dfs
        
        self.create_totals()
