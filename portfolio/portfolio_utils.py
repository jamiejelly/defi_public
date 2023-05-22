from web3 import Web3


ADDRESSES = {
    # addresses commented out
}

ADDRESSES = {k: Web3.toChecksumAddress(v) for k, v in ADDRESSES.items()}


def get_pending_cvx_rewards(crv_pending, cvx_token_contract):
    """
    find CVX/CRV ratio
    documentation: https://docs.convexfinance.com/convexfinanceintegration/cvx-minting  
    """
    cliffSize = 100000
    cliffCount = 1000
    maxSupply = 100000000
    cvxTotalSupply = float(Web3.fromWei(cvx_token_contract.functions.totalSupply().call(), unit="ether")) 
    currentCliff = cvxTotalSupply / cliffSize
    remaining = cliffCount  - currentCliff
    cvx_pending = crv_pending * remaining / cliffCount
    return cvx_pending


def get_total_glp(address, fsglp_contract, vglp_contract):
    """
    address; list
        list of addresses to check GLP risk for

    returns;
        total GLP (staked + vested), staked GLP total, vested GLP total
    """        
    # get staked GLP amount
    staked_glp = float(Web3.fromWei(fsglp_contract.functions.balanceOf(address).call(), unit="ether"))

    # get total vested GLP
    vested_glp = float(Web3.fromWei(vglp_contract.functions.pairAmounts(address).call(), unit="ether"))

    return staked_glp, vested_glp