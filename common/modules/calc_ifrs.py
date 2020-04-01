# adding attributes from the IFRS tables
import load_data
import numpy as np

# replacing GBV lower than 0 with 0 in order not to affecting split of caollaterals
df_dca = load_data.df_dca
df_dca.gross_balance_value = np.where(load_data.df_dca.gross_balance_value<=0, 0,load_data.df_dca.gross_balance_value)

df_ifrso_stats = load_data.df_ifrso[['pk_snapshot_date','pk_credit_account_id', 'begaap_provision_balance', 
    'begaap_provision_offbalance','begaap_classification']]
df_ifrso_stats['provision']=load_data.df_ifrso.begaap_provision_balance+ load_data.df_ifrso.begaap_provision_offbalance
df_ifrsi_stats = load_data.df_ifrsi[['pk_snapshot_date','pk_credit_account_id', 'default_flag', ]]

df_ifrs_stats = df_ifrso_stats.merge(df_ifrsi_stats, how = 'inner', left_on =['pk_snapshot_date', 
    'pk_credit_account_id'],right_on =['pk_snapshot_date', 'pk_credit_account_id'])

df_dca = df_dca.merge(df_ifrs_stats, how = 'inner', left_on ='pk_credit_account_id', right_on ='pk_credit_account_id')
df_dca['provision_coverage']=df_dca.provision / df_dca.gross_balance_value


