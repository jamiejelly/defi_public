import os
import json
import requests

from datetime import datetime
import pandas as pd

EXCHANGE_AVAIL = [
    'BINANCE',
    'BITFINEX',
    'COINBASE',
    'KRAKEN',
    'COINGECKO',
]

EXCHANGE_FREQ_UNITS = {
    'BINANCE': {'1m': '1m', '5m': '5m', '30m': '30m', '1D': '1d'},
    'BITFINEX': {'1m': '1m', '5m': '5m', '30m': '30m', '1D': '1D'},
    'COINBASE': {'1m': 60, '5m': 300, '30m': 1800, '1D': 86400},
    'COINGECKO': {'1m': None, '5m': None, '30m': None, '1D': None},
    'KRAKEN': {'1m': 1, '5m': 5, '30m': 30, '1D': 1440}
}

# REST API base URL
API_BASE = {
    'BINANCE': 'https://api.binance.com/api/v3',
    'BITFINEX': 'https://api-pub.bitfinex.com/v2',
    'COINBASE': 'https://api.pro.coinbase.com',
    'COINGECKO': 'https://api.coingecko.com/api/v3',
    'KRAKEN': 'https://api.kraken.com/0/public',
}

# OHLC URL endpoints
API_OHLC = {
    'BINANCE': lambda symb, freq: f'klines?symbol={convert_usd_to_usdt(symb)}&interval={freq}&limit=1000',  # max lim
    'BITFINEX': lambda symb, freq: f'candles/trade:{freq}:{symb}/hist',
    'COINBASE': lambda symb, freq: f'products/{symb}/candles?granularity={freq}',
    'COINGECKO': lambda symb, freq: f'coins/{symb}/ohlc?vs_currency=usd&days=365',  # defaut to 1 year vs USD
    'KRAKEN': lambda symb, freq: f'OHLC?pair={symb}&interval={freq}',
}

EXCH_SYMB_FORMAT = {
    'BINANCE': lambda b, q: f'{b}{q}',
    'BITFINEX': lambda b, q: f't{b}{q}',
    'COINBASE': lambda b, q: f'{b}-{q}',
    'COINGECKO': lambda b, q: f'{b}',
    'KRAKEN': lambda b, q: f'{b}{q}',
}

# convert one-off API symbs
CONVERT_API_SYMB = {
    'BITFNEX': {'BCH': 'BCHN:'}      # BCHUSD = BCHN:USD
}

ACCEPTABLE_TICKER_TIME_DELAY = 50  # seconds


class NoHistoricalPriceDataError(Exception):
    pass


class NoPerpAvailalableError(Exception):
    pass


def convert_usd_to_usdt(symb):
    if symb.endswith('USD'):
        return symb[:-3] + 'USDT'
    # else: 'USD"

##############################################################################################################################
##############################################################################################################################
##############################################################################################################################


"""
GET spot data from exchange
"""


def get_px_bfx(base, quote="USD"):
    try:
        return requests.get(f"https://api-pub.bitfinex.com/v2/ticker/t{base}{quote}").json()[-4]
    except BaseException as e:
        utils.logger.error(f"Failed to get spot position from Bitfinex for {base}|e={e}")
        return


def get_px_bin(base, quote="USDT"):
    if quote == "USD":
        quote = "USDT"
    try:
        j = requests.get(f"https://api3.binance.com/api/v3/ticker/price?symbol={base}{quote}").json()  # {base}USDT
        price = j["price"]
        if quote.upper() in ["USDT", "USD"]:
            usdt_usd_px = get_px_cbs("USDT", "USD")
            return float(price) * usdt_usd_px
        else:
            return float(price)
    except BaseException as e:
        utils.logger.error(f"Failed to get spot position from Binance for {base}|e={e}")
        return


def get_px_cbs(base, quote="USD"):
    try:
        j = requests.get(f"https://api.coinbase.com/v2/prices/{base}-{quote}/spot").json()
        if 'errors' not in j:
            return float(j["data"]["amount"])
    except BaseException as e:
        utils.logger.error(f"Failed to get spot position from Coinbase for {base}|e={e}")
        return


def get_px_ftx(base, quote="USD"):
    try:
        j = requests.get(f"https://ftx.com/api/markets/{base}/{quote}").json()
        if j['success']:
            return float(j["result"]["price"])
    except BaseException as e:
        utils.logger.error(f"Failed to get spot position from Ftx for {base}|e={e}")
        return


def get_cached_weighted_spot_px(base, quote, exchanges, break_at_first=False):
    cached_ticker_exchanges = [
        "Coinbase",
        # "Ftx",
        "Binance",
    ]
    pxs = []
    exchange_px = []
    for exchange in exchanges:
        if exchange not in cached_ticker_exchanges:
            continue

        # quote = "USDT" if (exchange == "Binance" and quote.upper() == "USD") else "USD"
        if quote.upper() == "USD":
            if exchange == "Binance":
                quote = "USDT"
        result = _get_cached_exchange_ticker_price(exchange, base + quote)
        price = result.get("price", None)
        if price is not None:
            pxs.append(price)
            exchange_px.append(exchange)
            if break_at_first:
                break
        utils.logger.error(f"[{exchange}]cached_price_unusable|skip_reason={result['error']}")
    if not pxs:
        utils.logger.debug(f"Spot position not found in cache for {base}")
    return pxs, exchange_px


def get_weighted_spot_px(base, quote, exchanges=['Coinbase', 'Binance', 'Bitfinex'], logging=False, break_at_first=False):
    funcs = {
        'Coinbase': get_px_cbs,
        # 'Ftx': get_px_ftx,
        'Binance': get_px_bin,
        'Bitfinex': get_px_bfx,
    }
    funcs = {k: v for k, v in funcs.items() if k in exchanges}

    pxs = []
    exchange_pxs = {}
    # Check Cache
    if CACHE:
        pxs, exchange_pxs = get_cached_weighted_spot_px(base, quote, exchanges, break_at_first=break_at_first)

    # If cache does not have prices or prices for all exchanges (break_at_first == False)
    if not pxs or not break_at_first:
        for venue, func in funcs.items():
            if venue in exchange_pxs:
                # Price retreived from cache already, skip
                continue

            px = func(base=base, quote=quote)
            if logging:
                utils.logger.debug(f'Spot {base}-{quote} at {venue}: \t{px}')
            if px is not None:
                pxs.append(px)
                if break_at_first:
                    break

    if not pxs:
        raise ValueError(f'did not find {base + quote} in {exchanges}')

    weighted_px = sum(pxs) / len(pxs)
    if logging:
        utils.logger.debug(f'Weighted {base}-{quote} spot px is {weighted_px}')
    return weighted_px


def get_ohlc(base, quote, freq='1D', read_csv=False):
    # if read_csv:
    #    return pd.read_csv(f'data/cache/ohlc/{symb}.csv', index_col=0)

    exchanges = ['BINANCE', 'COINBASE', 'KRAKEN', 'COINGECKO']
    for e in exchanges:
        lst = MarketDataGateway(e, base, quote, freq=freq).get_data()
        if lst:
            # utils.logger.debug(f'using {e} for {symb} ohlc data')
            ohlc = mk_data(lst)[['open', 'high', 'low', 'close']]
            ohlc.sort_index(inplace=True)
            if len(ohlc) >= 30:         # need 30d of data
                return ohlc
    raise NoHistoricalPriceDataError()


def _get_cached_exchange_ticker_price(exchange, pair):
    if not CACHE:
        return {"error": "cache not available"}
    ticker_data = CACHE.hgetall(f"ticker:{exchange}")
    timestamp = float(ticker_data.get("timestamp", 0))
    if datetime.now().timestamp() - timestamp > ACCEPTABLE_TICKER_TIME_DELAY:
        return {"error": "stale price"}
    price_data = json.loads(ticker_data.get("raw", {}))
    if pair not in price_data:
        return {"error": "pair not in exchange"}
    pair_price = price_data.get(pair, {}).get("price", None)
    if utils.is_convertible_to_float(pair_price):
        return {"price": float(pair_price)}
    return {"error": "missing price"}


##############################################################################################################################
##############################################################################################################################
##############################################################################################################################


"""
GET avail spot / futures data at exchanges

note: Binance - USDT margind perps
"""

# def get_futs_bin():
#     """USD margined"""
#     j = requests.get("https://testnet.binancefuture.com/fapi/v1/exchangeInfo").json()
#     symbs = []
#     for i in j['symbols']:
#         if i['contractType'] == 'PERPETUAL' and i['status'] == 'TRADING':
#             symbs.append((i['baseAsset'], f'{i["pair"]}-PERP', None))
#     return sorted(symbs)


# def get_futs_ftx():
#     symbs = []
#     j = requests.get('https://ftx.com/api/markets').json()
#     if j['success']:
#         for i in j['result']:
#             if i['enabled'] and i['type'] == 'future' and i['name'].endswith('PERP'):
#                 symbs.append((i['underlying'], i['name'], i['volumeUsd24h']))
#     return symbs


def get_market_bin(is_spot=True):
    if is_spot:
        symbs = []
        j = requests.get('https://api.binance.com/api/v3/exchangeInfo').json()
        for i in j['symbols']:
            if i['status'] == 'TRADING' and i['quoteAsset'] == 'USDT' and i['isSpotTradingAllowed']:
                symbs.append(i['baseAsset'])
        return symbs
    else:
        j = requests.get("https://testnet.binancefuture.com/fapi/v1/exchangeInfo").json()
        symbs = []
        for i in j['symbols']:
            if i['contractType'] == 'PERPETUAL' and i['status'] == 'TRADING':
                symbs.append((i['baseAsset'], f'{i["pair"]}-PERP', None))
        return sorted(symbs)


def get_market_cbs(is_spot=True):
    if is_spot:
        symbs = []
        j = requests.get('https://api.exchange.coinbase.com/currencies').json()
        for i in j:
            if i['status'] == 'online':
                symbs.append(i['id'])
        return symbs


def get_market_ftx(is_spot=True):
    if is_spot:
        symbs = []
        j = requests.get('https://ftx.com/api/markets').json()
        if j['success']:
            for i in j['result']:
                if i['enabled'] and i['type'] == 'spot' and i['quoteCurrency'] == 'USD':
                    symbs.append(i['baseCurrency'])
        return symbs
    else:
        symbs = []
        j = requests.get('https://ftx.com/api/markets').json()
        if j['success']:
            for i in j['result']:
                if i['enabled'] and i['type'] == 'future' and i['name'].endswith('PERP'):
                    symbs.append((i['underlying'], i['name'], i['volumeUsd24h']))
        return symbs


def get_avail_spot(exchanges=['BIN', 'CBS'], write_csv=''):
    funcs = {
        'BIN': get_market_bin,
        'CBS': get_market_cbs,
        # 'FTX': get_market_ftx,
    }
    symbs = []
    for e, func in funcs.items():
        tmp = func()
        for s in tmp:
            symbs.append(s)
    symbs_all = sorted(set(symbs))
    symbs_all = [i for i in symbs_all if 'BEAR' not in i and 'BULL' not in i and 'HALF' not in i and 'HEDGE' not in i]
    if write_csv:
        pd.Series(symbs_all).to_csv(write_csv)
        utils.logger.debug(f'wrote {write_csv}')
    utils.logger.debug(f'{len(symbs_all)} total avail spot symbs')
    return symbs_all


def get_avail_futs(symb=None, exchanges=['BIN']):
    funcs = {
        'BIN': get_market_bin,
        # 'FTX': get_market_ftx,
    }
    futs = {}
    for e in exchanges:
        e_avail = funcs[e](is_spot=False)
        for s, name, volm in e_avail:
            if s not in futs:
                futs[s] = {}
            futs[s][e] = name, volm

    if symb:
        if symb in futs:
            return futs[symb]
        else:
            raise NoPerpAvailalableError()
    return futs

##############################################################################################################################
##############################################################################################################################
##############################################################################################################################

"""
Example
df = mk_data(MarketDataGateway("BITFINEX", "BTC", "USD").get_data())

"""


def mk_data(lst):
    """
    Accepts standardized list of lists of unix timestamp (int) + OHLCV
    """
    df = pd.DataFrame(lst)
    df.set_index(0, inplace=True)
    df.index = [datetime.utcfromtimestamp(i).strftime('%Y-%m-%d %H:%M:%S') for i in df.index]
    df.sort_index(inplace=True)
    df.columns = ('open', 'high', 'low', 'close', 'volume')
    return df.astype(float)


def _get_cached_klines(exchange_name, base, quote, freq):
    return CACHE.get(f"options:klines:{exchange_name}:{base}:{quote}:{freq}")


class MarketDataGateway:

    def __init__(self, exch, base, quote, freq='1D'):
        """
        exch :: exchange to get data from
        base :: base of asset, ie BTC in BTC-USD
        quote :: quote of asset, ie USD in BTC-USD
        freq :: frequency of data, '1m', '5m', '30m' and '1D'
        """
        self.exch = exch
        self.base = base
        self.quote = quote
        self.freq = freq
        self._mk_symb()
        self._mk_url()

    def _mk_symb(self):
        self.symb = EXCH_SYMB_FORMAT[self.exch](self.base, self.quote)

    def _mk_url(self):
        base = API_BASE[self.exch]
        native_freq = EXCHANGE_FREQ_UNITS[self.exch][self.freq]

        # replace one-off API symbs
        symb_tmp = self.symb
        if CONVERT_API_SYMB.get(self.exch):
            symb_api = CONVERT_API_SYMB.get(self.exch).get(self.base)
            if symb_api:
                symb_tmp = self.symb.replace(self.base, symb_api)

        ext = API_OHLC[self.exch](symb_tmp, native_freq)
        self.url = os.path.join(base, ext)

    # separate into 2 functions
    # one for raw rata
    def get_raw_data(self) -> list:
        if self.exch == "COINGECKO":
            try:
                base = API_BASE[self.exch]
                coin_list = requests.get(base + '/coins/list').json()
                symbol_id = None
                for coin_dict in coin_list:
                    if self.base.lower() == coin_dict.get('symbol', '').lower():
                        symbol_id = coin_dict.get('id')
                        break
                ext = API_OHLC[self.exch](symbol_id, "")  # Coingecko OHLC url requires symbol id only
                self.url = os.path.join(base, ext)
            except Exception as e:
                utils.logger.error(f'failed to process {self.exch} symbol {self.base}|e={e}')
                return []

        r = requests.get(self.url).json()   # add logging

        if self.exch == 'BINANCE':
            # logging >>> object.__dict__
            if type(r) != list:     # returns {} if error
                return []

        if self.exch == 'COINBASE':
            # logging >>> object.__dict__
            if type(r) != list:     # returns {} if error
                return []

        elif self.exch == 'COINGECKO':
            # logging >>> object.__dict__
            if 'error' in r:
                return []

        elif self.exch == 'FTX':
            # logging >>> object.__dict__
            if not r['success']:
                return []

        elif self.exch == 'KRAKEN':
            # logging >>> object.__dict__
            if r['error']:
                return []

        return r

    def _make_dataframe(self, lst):
        """
        Accepts standardized list of lists of unix timestamp (int) + OHLCV
        """
        df = pd.DataFrame(lst)
        df.set_index(0, inplace=True)
        df.index = [datetime.utcfromtimestamp(i).strftime('%Y-%m-%d %H:%M:%S') for i in df.index]
        df.sort_index(inplace=True)
        df.columns = ('open', 'high', 'low', 'close', 'volume')
        return df.astype(float)

    def get_data(self) -> list:
        # Return timestamp, open, high, low, close, volume

        # Check cache
        if CACHE and self.freq == "1D":
            cached_out = []
            try:
                exchange_name = self.exch.lower().capitalize()
                cached_out = json.loads(_get_cached_klines(exchange_name, self.base, self.quote, self.freq))
                if cached_out:
                    # json.loads/dumps do not convert list to tuple and vice versa, so convert here to keep datatype format consistent
                    cached_out = [tuple(p) for p in cached_out]
                    return cached_out
            except Exception as e:
                utils.logger.error(f"failed to get data from cache|keyparams=({exchange_name},{self.base},{self.quote},{self.freq})|e={e}")

        j = self.get_raw_data()
        if not j:
            # logging.info(f'no data for {self.__dict__}')
            utils.logger.debug(f'no data for {self.__dict__}')
            return []

        if self.exch == 'BINANCE':
            out = [(i[0] / 1000, i[1], i[2], i[3], i[4], i[5]) for i in j]

        elif self.exch == 'BITFINEX':
            # returns time, open, close high low volume
            out = [(i[0] / 1000, i[1], i[3], i[4], i[2], i[5]) for i in j]

        elif self.exch == 'COINGECKO':
            # returns time, open, high, low, close
            out = [(i[0] / 1000, i[1], i[2], i[3], i[4], 0.) for i in j]

        elif self.exch == 'COINBASE':
            # returns time, 'low','high','open','close','volume'
            out = [(i[0], i[3], i[2], i[1], i[4], i[5]) for i in j]

        elif self.exch == 'FTX':
            out = [(i['time'] / 1000, i['open'], i['high'],
                    i['low'], i['close'], i['volume']) for i in j['result']]

        elif self.exch == 'KRAKEN':
            # returns different key, ie 'XXBTZUSD' for 'BTCUSD'
            j = j['result']
            j.pop('last')
            j = j[list(j.keys())[0]]
            # [time, open, high, low, closes, vwap, volume, count]
            out = [(i[0], i[1], i[2], i[3], i[4], i[6]) for i in j]

        else:
            raise ValueError()

        return out

    def make_data(self):
        lst = self.get_data()
        if lst:
            return self._make_dataframe(lst)
