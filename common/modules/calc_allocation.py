import calc_ifrs, calc_collaterals, calc_valuation,load_data
from functions import exposure_class_pre


# df_dca.provision = np.where(df_dca.bega<=0, 0,df_dca.gross_balance_value)
df_obl_tot = calc_ifrs.df_dca[['obligor_id', 'gross_balance_value']]
df_obl_tot['total'] = df_obl_tot.groupby(by=['obligor_id']).transform('sum')
df_obl_tot = df_obl_tot[['obligor_id','total']].drop_duplicates()
df_obl_acc = calc_ifrs.df_dca[['obligor_id', 'gross_balance_value', 'temenos_account_id','product_id']]
df_obl_acc = df_obl_acc.merge(df_obl_tot, how='inner', left_on='obligor_id', right_on ='obligor_id')
df_obl_acc['share_gbv']=df_obl_acc.gross_balance_value/df_obl_acc.total

df_coll_obl = calc_collaterals.df_alloc_coll[['credit_collateral_id','credit_account_id', 'valuation_value','value','property_id','collateral_type_id', 'collateral_status_id']]
df_coll_obl = df_coll_obl.merge(df_obl_acc[['temenos_account_id','obligor_id']], how = 'left', left_on ='credit_account_id', 
                                right_on = 'temenos_account_id')
df_coll_obl = df_coll_obl.groupby(by=['credit_collateral_id','obligor_id','collateral_type_id'], as_index=False).agg({'value':'max', 'valuation_value':'max'})
df_coll_obl = df_coll_obl.groupby(by=['obligor_id','collateral_type_id'], as_index=False).agg({'value':'sum', 'valuation_value':'sum'})
print(df_coll_obl[df_coll_obl.obligor_id=='18127'])


df_obl_acc_coll = df_obl_acc.merge(df_coll_obl, how='left', left_on='obligor_id', right_on='obligor_id').fillna(0)
df_obl_acc_coll['allocated_collateral'] = df_obl_acc_coll.value * df_obl_acc_coll.share_gbv
df_obl_acc_coll=df_obl_acc_coll.merge(load_data.df_dp[['pk_product_id','product_variant_code']], how = 'left', left_on='product_id', right_on='pk_product_id', suffixes=['','_y'])
#==========================================================================================================================================================
#results of mortgages allocation
#==========================================================================================================================================================
df_obl_acc_coll_occupied = df_obl_acc_coll.merge(load_data.df_cmi_merged, how = 'left', left_on='temenos_account_id', right_on='T_24').fillna('no_info')
df_obl_acc_coll_occupied.to_csv('collaterals_100_200_split')
df_obl_acc_coll_occupied.collateral_type_id = df_obl_acc_coll_occupied.collateral_type_id.astype(int) 
df_obl_acc_coll_occupied.product_variant_code = df_obl_acc_coll_occupied.product_variant_code.astype('category') 

df_obl_acc_coll_occupied['rwa_class_pre'] = df_obl_acc_coll_occupied.product_variant_code.apply(exposure_class_pre)
df_obl_acc_coll_occupied['pk_snapshot_date'] = '2019-12-31'
df_obl_acc_coll_occupied.drop('pk_product_id', axis=1, inplace=True)
df_obl_acc_coll_occupied.columns=[
       'obligor_id', 'gbv_per_account', 'temenos_account_id', 'product_id',
       'gbv_total_obligor', 'share_gbv_contract_in_total',
       'collateral_type_id', 'value_of_mortgage', 'value_of_property',
       'value_of_allocated_mortgage', 'product_variant_code', 'T_24',
       'occupied_by_owner', 'linked_credit_quality', 'repayment_capacity',
       'is_eligible_collateral', 'rwa_class_pre', 'pk_snapshot_date']



print(df_obl_acc_coll_occupied.columns)
df_obl_acc_coll_occupied.to_csv('test.csv', index=False)
# df_obl_acc_coll_occupied.to_csv('collaterals.csv')
