import load_data
import functions

df_valuation=load_data.df_dccv[[
    'valuation_date',
    'valuation_value',
    'property_id',
    'credit_collateral_id',
    'valuation_status',
    'valuation_type_id',
]].query("valuation_status==True")

# print(df_valuation[df_valuation.property_id==954])
# print(functions.find_valuation(df_valuation,954))
# print('a')