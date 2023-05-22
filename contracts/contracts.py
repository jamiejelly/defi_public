import json
import time
from collections import namedtuple

import requests
import web3
from bs4 import BeautifulSoup
from web3 import Web3, HTTPProvider

def address_2_checksum(address):
    return Web3.toChecksumAddress(address)

# Alchemy free RPC's
NETWORK_2_RPC = {
    "ARBI": "https://arb-mainnet.g.alchemy.com/v2/<KEY>",
    "ETH": "https://rpc.ankr.com/eth", 
    "OP": "https://rpc.ankr.com/optimism", # have multiple in case you get rate-limited
}

NETWORK_2_EXPLORER = {
    "ARBI": lambda contract, token_or_address: f"https://arbiscan.io/{token_or_address}/{contract}#code",
    "ETH": lambda contract, token_or_address: f"https://etherscan.io/{token_or_address}/{contract}#code",
    "OP": lambda contract, token_or_address: f"https://optimistic.etherscan.io/{token_or_address}/{contract}#code",
}

NETWORK_2_EXPORER_API = {
    "ARBI": lambda contract, key: f"https://api.arbiscan.io/api?module=contract&action=getabi&address={contract}&apikey={key}",
    "ETH": lambda contract, key: f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract}&apikey={key}",
    "OP": lambda contract, key: f"https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address={contract}&apikey={key}",
}

NETWORK_2_API_KEY = {
    "ARBI": "<KEY>",
    "ETH": "<KEY>",
    "OP": "<KEY>",
}

# address, is_token (bool)
ADDRESS_ETHEREUM = {
    "crv": ("0xD533a949740bb3306d119CC777fa900bA034cd52", True),
}

ADDRESS_ARBITRUM = {
    "arb": ("0x912CE59144191C1204E64559FE8253a0e49E6548", True),
    "dai": ("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", True),
    "frax": ("0x17FC002b466eEc40DaE837Fc4bE5c67993ddBd6F", True),
    "esgmx": ("0xf42Ae1D54fd613C9bb14810b0588FaAa09a426cA", True), # esGMX spot
    "gmx": ("0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a", True),
    "vgmx": ("0x199070ddfd1cfb69173aa2f7e20906f26b363004", True), # esGMX vesting
    "link": ("0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", True),
    "uni": ("0xFa7F8980b0f1E64A2062791cc3b0871572f1F7f0", True),
    "usdc": ("0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", True),
    "usdt": ("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", True),
    "wbtc": ("0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", True),
    "weth": ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", True),
    "glp_manager": ("0x3963FfC9dff443c2A94f21b129D429891E32ec18", False),
    "vault": ("0x489ee077994B6658eAfA855C308275EAd8097C4A", False),
    "fglp": ("0x4e971a87900b931ff39d1aad67697f49835400b6", False),
    "fsglp": ("0x1addd80e6039594ee970e5872d247bf0414c8903", False),
    "vglp": ("0xA75287d2f8b217273E7FCD7E86eF07D33972042E", False),
}

ADDRESS_2_PROXY = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "0xa2327a938febf5fec13bacfb16ae10ecbc4cbdcf", # USDC ETH
    "0x912CE59144191C1204E64559FE8253a0e49E6548": "0xC4ed0A9Ea70d5bCC69f748547650d32cC219D882", # ARB ARBI
}

ADDRESS_2_PROXY = {address_2_checksum(k) : address_2_checksum(v) for k, v in ADDRESS_2_PROXY.items()}

# named tuple
ContractSpec = namedtuple('Contract', ['address', 'is_token'])

def convert_keys_2_named_tuple(dct):
    d = dict(dct)
    return {k: ContractSpec(address=address_2_checksum(add), is_token=bool_) for k, (add, bool_) in d.items()}

ADDRESS_BOOK = {
    "ARBI": convert_keys_2_named_tuple(ADDRESS_ARBITRUM),
    "ETH": convert_keys_2_named_tuple(ADDRESS_ETHEREUM),
}


# move to utils?
def convert_balance_2_units(balance, unit="ether"):
    return float(Web3.fromWei(balance, unit=unit))

# convert to checksum address
# def address_2_checksum(dict_):
#     dict_ = dict(dict_)
#     for network, d in dict_.items():
#         for token, add in d.items():
#             dict_[network][token] = Web3.toChecksumAddress(add)
#     return dict_

# instantiate a web3 remote provider
def init_w3_provider(network):
    return Web3(Web3.HTTPProvider(NETWORK_2_RPC[network]))

class ContractFactory:
    
    def __init__(self, network: str):
        """
        :param network: ARBI, AVAX, ETH, POLYGON
        """
        self.network = network.upper()
        self.w3 = init_w3_provider(self.network)

    def create_contract(self, address, is_token: bool, key_2_get_abi=False, proxy_contract_address=None):
        address = Web3.toChecksumAddress(address)
        address_abi = address
        if proxy_contract_address:
            address_abi = Web3.toChecksumAddress(proxy_contract_address)
        
        if key_2_get_abi:
            abi = self.get_contract_abi_w_api(address_abi, key_2_get_abi)
        else:
            abi = self.get_contract_abi(address_abi, 'token' if is_token else 'address')
        
        contract = self.w3.eth.contract(
            address=address, 
            abi=abi,
        )
        return contract
    
    def get_contract_abi(self, address, is_token: bool):
        url = NETWORK_2_EXPLORER[self.network](address, is_token)
        data = requests.get(url, headers = {'User-Agent': 'Popular browser\'s user-agent'})
        html = BeautifulSoup(data.text, 'html.parser')
        abi_html = html.find_all(class_="wordwrap js-copytextarea2")[0]
        abi_json = json.loads(abi_html.next_element)
        return abi_json

    def get_contract_abi_w_api(self, address, key):
        # MAX 5 per seconds
        r = requests.get(NETWORK_2_EXPORER_API[self.network](address, key), headers = {'User-Agent': 'Popular browser\'s user-agent'})
        abi = list(json.loads(r.json()['result']))
        time.sleep(0.5)
        return abi
    
    
CONTRACT_FACTORIES = {
    'ARBI': ContractFactory('ARBI'),
    'ETH': ContractFactory('ETH'),
}
