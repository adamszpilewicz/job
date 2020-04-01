import load_data
import functions
import calc_valuation

df_alloc=load_data.df_dcca[['pk_snapshot_date','pk_credit_collateral_allocation_id','credit_account_id', 'credit_collateral_id',
    'allocation_amount']]
df_coll=load_data.df_dcc[['pk_snapshot_date','property_id','pk_credit_collateral_id','value', 'collateral_type_id', 'collateral_status_id']]
# df_coll['collateral_valuation']= df_coll.apply(functions.find_valuation,args=((calc_valuation.df_valuation, df_coll.property_id),), axis=1)#    functions.find_valuation(calc_valuation.df_valuation,df_coll.property_id)
# print(df_coll.head())
df_alloc_coll = df_alloc.merge(df_coll, how = 'inner', left_on='credit_collateral_id', right_on='pk_credit_collateral_id', suffixes=['','_y'])
mask = (df_alloc_coll.collateral_status_id==2) & (df_alloc_coll.collateral_type_id.isin([100,200]))
df_alloc_coll=df_alloc_coll[mask]
df_alloc_coll=df_alloc_coll.dropna(subset=['credit_account_id','pk_snapshot_date'])
df_alloc_coll.credit_account_id = df_alloc_coll.credit_account_id.astype(int)
df_alloc_coll = df_alloc_coll.merge(df_coll, how = 'inner', left_on='credit_collateral_id', right_on='pk_credit_collateral_id', 
    suffixes=(['','_y']))
df_alloc_coll=df_alloc_coll.dropna(subset=['credit_account_id','pk_snapshot_date'])
df_alloc_coll=df_alloc_coll.merge(load_data.df_dccv[['property_id', 'valuation_value', 'valuation_status'] ].query("valuation_status==True"), 
    how='left', left_on='property_id', suffixes=['','dccv'],right_on='property_id')

# df_alloc=load_data.df_dcca[['pk_snapshot_date','pk_credit_collateral_allocation_id','credit_account_id', 'credit_collateral_id',
#     'allocation_amount']]
# df_coll=load_data.df_dcc[['pk_snapshot_date','property_id','pk_credit_collateral_id','value', 'collateral_type_id','collateral_status_id']]
# df_alloc_coll = df_alloc.merge(df_coll, how = 'inner', left_on='credit_collateral_id', right_on='pk_credit_collateral_id', suffixes=['','_y'])
# mask = (df_alloc_coll.collateral_status_id==2) & (df_alloc_coll.collateral_type_id.isin([100,200]))
# df_alloc_coll=df_alloc_coll[mask]
# df_alloc_coll=df_alloc_coll.dropna(subset=['credit_account_id','pk_snapshot_date'])
# df_alloc_coll.credit_account_id = df_alloc_coll.credit_account_id.astype(int)
# df_alloc_coll = df_alloc_coll.merge(df_coll, how = 'inner', left_on='credit_collateral_id', right_on='pk_credit_collateral_id', 
#     suffixes=(['','_y']))
# df_alloc_coll=df_alloc_coll.dropna(subset=['credit_account_id','pk_snapshot_date'])
# df_alloc_coll=df_alloc_coll.merge(load_data.df_dccv[['property_id', 'valuation_value', 'valuation_status'] ].query("valuation_status==True"), 
#     how='left', left_on='property_id', suffixes=['','dccv'],right_on='property_id')
print(sorted(df_alloc_coll.columns))
# mask = df_alloc_coll.credit_account_id.isin([1000431587,1000430389])
