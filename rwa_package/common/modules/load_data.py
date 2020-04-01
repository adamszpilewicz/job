import pandas as pd
import os
from functions import return_directory, is_eligible
import numpy as np
#==========================================================================================================================================================
#reading csv
#==========================================================================================================================================================
df_dca   =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dca.csv'))
df_foc   =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_foc.csv'))
df_ifrsi =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_ifrsi.csv'))
df_ifrso =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_ifrso.csv'))
df_dbc   =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dbc.csv'))
df_dcc   =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dcc.csv'))
df_dcct  =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dcct.csv'))
df_dccv  =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dccv.csv'))
df_dcca  =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dcca.csv'))
df_dcci  =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dcci.csv'))
df_dccis =pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dccis.csv'))
df_dp = pd.read_csv(os.path.join(return_directory(level_up=0),'csv/df_dp.csv'))
#Carlo
df_cmi = pd.read_csv(os.path.join(return_directory(level_up=0),'csv/MortgagesInfo.csv'),usecols=[*(range(6))]).fillna(0)

# df_cmi = pd.read_csv(fullpath, usecols=[*(range(6))]).fillna(0)
df_cmi.columns=['T24_1','T24_2','T24_3','occupied_owner','linked_credit_quality','repayment_capacity']
col_names = ['T_24','occupied_owner','linked_credit_quality','repayment_capacity']
df_cmi_1 = df_cmi.iloc[:,2:]
df_cmi_1.columns = ['T_24','occupied_owner','linked_credit_quality','repayment_capacity']
df_cmi_2 = df_cmi.iloc[:,[1,3,4,5]]
df_cmi_2.columns = ['T_24','occupied_owner','linked_credit_quality','repayment_capacity']
df_cmi_3 = df_cmi.iloc[:,[0,3,4,5]]
df_cmi_3.columns = ['T_24','occupied_owner','linked_credit_quality','repayment_capacity']
df_cmi_merged=df_cmi_1.append(df_cmi_2).append(df_cmi_3).drop_duplicates()
df_cmi_merged.occupied_owner = df_cmi_merged.occupied_owner.replace({'oui':'yes', 'NON':'no', 'OUI': 'yes', 'non':'no'})
df_cmi_merged.linked_credit_quality = df_cmi_merged.linked_credit_quality.replace({'oui':'yes', 'NON':'no', 'OUI': 'yes', 'non':'no'})
df_cmi_merged.repayment_capacity = df_cmi_merged.repayment_capacity.replace({'oui':'yes', 'NON':'no', 'OUI': 'yes', 'non':'no'})
#remove all 0 contracts 
mask_1 = df_cmi.T24_1!=0
mask_2 = df_cmi.T24_2!=0
mask_3 = df_cmi.T24_3!=0
df_cmi_merged = df_cmi_merged[(mask_1) & (mask_2) & (mask_3)]
df_cmi_merged['is_eligble'] = df_cmi_merged.apply(lambda x: is_eligible(x['linked_credit_quality'], x['repayment_capacity']), axis=1)
