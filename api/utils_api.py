import os
import time
from urllib.parse import urlencode

import requests

EXCHANGE_TO_BASE_URL = {
    'BINANCE': 'https://api.binance.com/api/v3',
    'BINANCE_FUTURES_USDM': 'https://fapi.binance.com/fapi/v1',
    'BINANCE_FUTURES_COINM': 'https://dapi.binance.com',
    'BINANCE_US': 'https://api.binance.us',
    'BITFINEX': 'https://api-pub.bitfinex.com/v2',
    'COINBASE': 'https://api.pro.coinbase.com',
    'COINGECKO': 'https://api.coingecko.com/api/v3',
    'DEFILLAMA': 'https://api.llama.fi',
    'DERIBIT': 'wss://www.deribit.com/ws/api/v2',
    'KRAKEN': 'https://api.kraken.com/0/public',
}


def make_endpoint(endpoint, **args):
    return endpoint + '?' + urlencode(args) if args else endpoint


def make_get_request(url: str, endpoint: str, **args):
    """
    :param url: base url of API 
    :param endpoint: endpoint of API to be called
    :params **args: urlencodes args, ie endpoint + ? arg1=val1 & arg2=val2
    """
    return requests.get(os.path.join(url, make_endpoint(endpoint, **args))).json()


def get_timestamp_ms():
    """
    get timestamp in milliseconds
    """
    return int(round(time.time() * 1000))