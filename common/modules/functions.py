from pathlib import Path
import os
import pandas as pd

def is_eligible(credit_quality, repayment_capacity):
    """assigning risk weights for exposures
    
    Arguments:
        credit_quality {str} -- is the value of the property significantly linked to the credit quality of the borrower
        repayment_capacity {str} -- does the repayment capacity come substantially from a flow from this property
    """
    if credit_quality=='yes' or repayment_capacity=='yes':
        return 0
    return 1

def return_directory(full_path=os.path.dirname( __file__ ), level_up = 0):
    return str(Path(full_path).parents[level_up]) 

def exposure_class_pre(product_variant_code):
    """defining rwa exposure class before collateral assignment
    
    Arguments:
        product_variant_code {str} -- product variant code of the account
    """
    return product_variant_code.split('_')[0] 

def exposure_class_post(df):
    """calculation of post-mitigation exposure class
    
    Arguments:
        df {pandas DataFrame} -- dataframe with exposures with assigned collaterals
    """
    rwa_post_info = pd.DataFrame(columns=['temenos_account_id','rwa_class_post','gross_balance_value_post' ])
    for i in range(0, len(df)):
        row = df.iloc[[i]] 
        _id = row.temenos_account_id.item()
        if row.collateral_type_id.item() == 0:
            _class_post = row.rwa_class_pre.item()
            _gbv_post = row.gross_balance_value.item()
        elif row.allocated_collateral.item() > row.gross_balance_value.item():
            _class_post = row.collateral_type_id.item()
            _gbv_post = row.gross_balance_value.item()
        else:
            _class_post = [row.rwa_class_pre.item(),  row.collateral_type_id.item()]
            _gbv_post = [row.gross_balance_value.item() - row.allocated_collateral.item(), row.allocated_collateral.item()]
        try:
            _df_to_append = pd.DataFrame({'temenos_account_id': _id,
                                'rwa_class_post': _class_post,
                                'gross_balance_value_post': _gbv_post}, index=[0])
        except ValueError:
                        _df_to_append = pd.DataFrame({'temenos_account_id': _id,
                                'rwa_class_post': _class_post,
                                'gross_balance_value_post': _gbv_post})

        rwa_post_info = rwa_post_info.append(_df_to_append)
    return rwa_post_info

def find_valuation(df_valuation, prop_id):
    """function for assigning property valuation to collateral
    
    Arguments:
        df_valuation {[type]} -- [description]
        property_id {[type]} -- [description]
    """
    return df_valuation[df_valuation.property_id==prop_id].property_id.item()





    
