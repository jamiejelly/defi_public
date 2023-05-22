import os
import sys
sys.path.append('/'.join(os.getcwd().split('/')[:-1])) # absolute path

import contracts
from contracts import address_2_checksum, convert_balance_2_units

# create crv contracts
networkFactory = contracts.CONTRACT_FACTORIES['<NETWORK>']
ADDRESSES = contracts.ADDRESS_BOOK['<NETWORK>']
crv_contracts_needed = [
    'crv',
    # contracts ..
]
crv_addresses = {k: v for k, v in ADDRESSES.items() if k in crv_contracts_needed}
CONTRACTS = {k: networkFactory.create_contract(*v) for k, v in crv_addresses.items()}


def calc_risk_of_crv_pool(our_address, our_pool):
    """
    params:
        our_address (str) :: address that holds LP tokens
        our_pool (srt) :: address of CRV pool
    
    returns:
        total_risk_crv_pool_native (dict) :: total risk assets in pool
        
        our_risk_crv_pool_native (dict) :: our share of risk assets in pool given [[our_address]]

    """
    
    our_address = address_2_checksum(our_address)
    
    ########### inspect POOL_1
    # 0 index = <TOKEN>
    # 1 index = <LP_TOKEN>
    risk_token_1 = convert_balance_2_units(CONTRACTS[our_pool].functions.balances(0).call())
    lp_token_1 = convert_balance_2_units(CONTRACTS[our_pool].functions.balances(1).call())

    ########### inspect POOL_2
    # and what are there balances?
    lp2_token_1 = convert_balance_2_units(CONTRACTS['<POOL_NAME_2>'].functions.balances(0).call())
    lp2_token_2 = convert_balance_2_units(balance=CONTRACTS['<POOL_NAME_2>'].functions.balances(1).call(), unit='gwei') * 10
    lp2_token_3 = convert_balance_2_units(CONTRACTS['<POOL_NAME_2'].functions.balances(2).call())

    # what % of POOL_2 is in POOL_1?
    lp_token_1_total = convert_balance_2_units(CONTRACTS['<POOL_TOKEN_2>'].functions.totalSupply().call())
    lp_token_1_pct = lp_token_1 / lp_token_1_total
    lp_token_1_pct * lp2_token_2
    lp_token_1_pct * lp2_token_3

    ########### inspect POOL_3
    dai = convert_balance_2_units(CONTRACTS['<POOL_NAME_3>'].functions.balances(0).call())
    usdc = convert_balance_2_units(CONTRACTS['<POOL_NAME_3>'].functions.balances(1).call(), unit="mwei")
    usdt = convert_balance_2_units(CONTRACTS['<POOL_NAME_3>'].functions.balances(2).call(), unit="mwei")

    # what % of POOL_3 is in POOL_2?
    pool3_risk_total = convert_balance_2_units(CONTRACTS['<POOL_TOKEN_3>'].functions.totalSupply().call())
    pool3_risk_pct = lp2_token_1 / pool3_risk_total
    stables_risk = (dai + usdc + usdt) * pool3_risk_pct * lp_token_1_pct

    # Total native risk in pool
    risk_pool = {
        "USD": stables_risk,
        "TOKEN_1": risk_token_1,
        "TOKEN_2": lp_token_1_pct * lp2_token_2,
        "TOKEN_3": lp_token_1_pct * lp2_token_3,
    }
    risk_pool.pop("USD")

    # Our crv / atricypto3 risk native
    our_supply_lp = CONTRACTS['<POOL_GAUGE>'].functions.balanceOf(our_address).call()
    total_supply_lp = CONTRACTS['<POOL_TOKEN>'].functions.totalSupply().call()
    our_pool_pct = our_supply_lp / total_supply_lp
    our_risk_crv_pool_native = {k: v * our_pool_pct for k, v in total_risk_crv_pool_native.items()}
    
    return total_risk_crv_pool_native, our_risk_crv_pool_native

