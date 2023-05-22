import json
import logging
import os
import sys
import time

import requests
from web3 import Web3, HTTPProvider

sys.path.append(os.path.join('/'.join(os.getcwd().split('/')[:-1]), 'contracts'))
import contracts
from contracts import ADDRESS_BOOK

logger = logging.getLogger('price_risk')
logger.setLevel(logging.INFO)


GMX_TOKENS = {
    "ARBI": ["DAI","FRAX","USDC","USDT","WBTC","WETH", "UNI", "LINK"],
    "AVAX": ["USDC", "USDC.e", "BTC.b", "WBTC.e", "WETH.e", "WAVAX"],
}

TRADER_BIAS_TOKENS = {
    "ARBI": ["WBTC", "WETH", "LINK", "UNI"],
    "AVAX": ["BTC.b", "WBTC.e", "WETH.e", "WAVAX"]    
}

PENDING_REWARDS = {
    "ARBI": "ETH",
    "AVAX": "AVAX",
}

BASE_URL = {
    "ARBI": "https://api.gmx.io",
    "AVAX": "https://gmx-avax-server.uc.r.appspot.com",
}

WRAPPERS_2_NATIVE = {
    'USDC.e': 'USDC',
    'WBTC.e': 'BTC',
    'WBTC': 'BTC',
    'BTC.b': 'BTC',
    'WETH': 'ETH',
    'WETH.e': 'ETH',
    'WAVAX': 'AVAX',
}


def map_native_2_wrapper(d):
        out = dict(d)
        out = {k.upper(): v for k, v in out.items()}
        for key_replace, key in WRAPPERS_2_NATIVE.items():
            if key_replace in out:
                out[key] = out[key_replace]
                # out.pop(key_replace)
        return out


class GmxRisk:

    CONTRACTS_DEPLOYED = None # {"ARBI": {}, "AVAX": {}}
    SYMBOL_2_GLP_USD_RISK = {"ARBI": {}, "AVAX": {}}
    SYMBOL_2_GLP_PCT_RISK = {"ARBI": {}, "AVAX": {}}

    def __init__(self, address, network, cmap, sleep=False, logging=True):
        self.address = address
        self.network = network
        self.cmap = cmap
        self.sleep = sleep
        self.logging = logging

        # urls based on network
        self.price_url = os.path.join(BASE_URL[self.network], "prices")
        self.position_url = os.path.join(BASE_URL[self.network], "position_stats")
        self.token_url = os.path.join(BASE_URL[self.network], "tokens")

        if self.logging:
            logger.info(f"Address: {self.address} --> {self.network} NETWORK")

    def get_spot_prices(self):
        glp_price_request = requests.get(self.price_url).json()
        add_2_token = {v.address: k for k, v in ADDRESS_BOOK[self.network].items()}
        spot_prices = {add_2_token[k]: float(Web3.fromWei(int(v), unit='Tether')) for k, v in glp_price_request.items()}

        # set all wrapped tokens = native
        spot_prices = map_native_2_wrapper(spot_prices)

        # GLP price
        spot_prices["GLP"] = float(Web3.fromWei(
            self.cmap[self.network]['glp_manager'].functions.getPrice(True).call(), unit="Tether"))
        
        spot_prices = {k.upper(): v for k, v in spot_prices.items()}

        return spot_prices

    # get token GLP composition %
    def get_glp_token_composition_pct(self):
        glp_total_usd = 0
        for symb in GMX_TOKENS[self.network]:
            if symb not in self.SYMBOL_2_GLP_USD_RISK[self.network]:
                add = ADDRESS_BOOK[self.network][symb.lower()].address
                symb_usd_risk = float(Web3.fromWei(
                    self.cmap[self.network]['vault'].functions.usdgAmounts(add).call(), unit="ether"))
                self.SYMBOL_2_GLP_USD_RISK[self.network][symb] = symb_usd_risk
            else:
                symb_usd_risk = self.SYMBOL_2_GLP_USD_RISK[self.network][symb]
            glp_total_usd += symb_usd_risk

        symbol_2_glp_pct_risk = {k: v / glp_total_usd for k, v in self.SYMBOL_2_GLP_USD_RISK[self.network].items()}
        if self.logging:
            logger.info(f"symbol_2_glp_pct_risk: {json.dumps(({k: round(v * 100, 2) for k, v in symbol_2_glp_pct_risk.items()}),indent=4)}")

        return symbol_2_glp_pct_risk

    def get_glp_risk(self):
        """
        addresses; list
            list of addresses to check GLP risk for

        returns;
            total GLP (staked + vested), staked GLP total, vested GLP total
        """        
        # get staked GLP amount
        staked_glp = float(Web3.fromWei(
            self.cmap[self.network]['fsglp'].functions.balanceOf(self.address).call(), unit="ether"))

        # get total vested GLP
        vested_glp = float(Web3.fromWei(
            self.cmap[self.network]['vglp'].functions.pairAmounts(self.address).call(), unit="ether"))

        return staked_glp, vested_glp

    def get_pending_rewards(self):
        # native pending reward 
        #  (ETH for ARBI, AVAX for AVAX)
        pending_reward_native = {
            PENDING_REWARDS[self.network]: float(Web3.fromWei(
                self.cmap[self.network]['fglp'].functions.claimable(self.address).call(), unit="ether"))
        }
        return pending_reward_native

    ###################### Trader risk bias

    def get_token_utilization_pct(self, symb):
        return self.cmap[self.network]['vault']\
            .functions.getUtilisation(ADDRESS_BOOK[self.network][symb.lower()].address).call() / 1e6    

    def main(self):
        # init contracts
        # self.init_contracts()

        # get spot prices from GMX site
        spot_prices = self.get_spot_prices()

        # GLP price risk
        sglp, vglp = self.get_glp_risk()
        glp_risk_usd = (sglp + vglp) * spot_prices['GLP']

        # native pending reward 
        #  (ETH for ARBI, AVAX for AVAX)
        pending_reward_native = self.get_pending_rewards()

        # {symbol : GLP % composition }
        # {symbol : GLP native risk based on our GLP position}
        symb_2_glp_pct_risk = self.get_glp_token_composition_pct()
        symb_2_glp_native_risk = {k: glp_risk_usd * v / spot_prices[k] for k, v in symb_2_glp_pct_risk.items()}

        
        # temp
        self.spot_prices = spot_prices
        self.glp_risk_usd = glp_risk_usd
        self.symb_2_glp_pct_risk = symb_2_glp_pct_risk
        self.symb_2_glp_native_risk = symb_2_glp_native_risk
        
        # trader bias risk
        #  (commented out)
        trader_bias_price_risk = None # self.get_trader_bias_risk()

        return {
            'glp_native_risk': symb_2_glp_native_risk,
            'glp_risk': sglp + vglp,
            'glp_usd_risk': glp_risk_usd,
            'pending_rewards': pending_reward_native,
            'trader_bias': trader_bias_price_risk,
        }
