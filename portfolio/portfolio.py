"""
Redesigned portfolio\
(Automated ABIs, etc …)

*Equity
a) Balances (Defi / Binance) 

*Alerting
a) job - Margin in accounts 
b) job - Portfolio price risk
c) trigger - high funding rates (in either direction)
d) trigger - price risk $ > limit 

"""

from collections import defaultdict
from configparser import ConfigParser
import json
import os
import logging
import sys
from client import TelegramBotHandler
sys.path.append('/'.join(os.getcwd().split('/')[:-1])) # absolute path

import pandas as pd
import requests
from web3 import Web3

import api
import config
import contracts
import crv_utils
import gmx_utils
import portfolio_utils
import utils
from vault import VaultClient
from client import TelegramBotHandler

GMX_ADDRESSES = [
    portfolio_utils.ADDRESSES['GMX_INST_1'],
    portfolio_utils.ADDRESSES['GMX_INST_2'],
]

# logging
logger = logging.getLogger('price_risk')
logger.setLevel(logging.INFO)

if __name__ == "__main__":

    ###################### INIT LOGGER
    log_name = "defi_portfolio"
    stream = open(f"{log_name}.log", "a")
    stream_handler = logging.StreamHandler(stream)
    stream_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    logger.addHandler(stream_handler)
    logging.getLogger("urllib3.util.retry").disabled = True
    logging.getLogger("urllib3.util").disabled = True
    logging.getLogger("urllib3").disabled = True
    logging.getLogger("urllib3.connection").disabled = True
    logging.getLogger("urllib3.response").disabled = True
    logging.getLogger("urllib3.connectionpool").disabled = True
    logging.getLogger("urllib3.poolmanager").disabled = True
    logging.getLogger("requests").disabled = True
    ##########Telegram Msg Send ################
    telegram_handler = TelegramBotHandler()
    telegram_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
       )
    logger.addHandler(telegram_handler)
    
    ################################# Load prices
    #################################

    spot_symbs = ["BTC", "ETH"] # alts not inluded
    binance_prices = requests.get(f"https://api.binance.com/api/v3/ticker/price").json()
    spot_prices = {s: float(d['price']) for d in binance_prices for s in spot_symbs if d['symbol'] == f'{s}USDT'}
    
    ################################# Binance data
    #################################
    
    ################################# connect binance clients
    bgs = []
    for account_ in config.BINANCE_ACCOUNTS:
        vc = VaultClient(binance_vault_path=account_)
        err, resp = vc.get_binance_api_secret()
        bgs.append(api.BinanceClient(
            'BINANCE_FUTURES_USDM', 
            resp.get("BINANCE_API_KEY"), 
            resp.get("BINANCE_API_SECRET"),
        ))

    ################################# connect binance equity
    futs_lst = defaultdict(list)
    wallet_lst = defaultdict(list)
    urlz_lst = defaultdict(list) 

    for bg in bgs:
        account_ = bg.get_account()

        # futures positions
        futs_ = bg.get_open_futs_positions()
        [futs_lst[k].append(v) for k, v in futs_.items()]

        # unrealized pnl
        urlz_ = {
            d['symbol']: float(d['unrealizedProfit'] )
            for d in account_['positions']
            if abs((float(d['unrealizedProfit']))) > 0
        }
        [urlz_lst[k].append(v) for k, v in urlz_.items()]

        # margin balance = wallet balance + urlz pnl (w the haircut)
        # liquidation value (equity) of the account is the sum of all tokens * spot
        wallet_ = {
            d['asset']: float(d['walletBalance']) 
            for d in account_['assets'] 
            if abs(float(d['walletBalance'])) > 0
        }
        [wallet_lst[k].append(v) for k, v in wallet_.items()]

        wallet_usd_ = sum({k: v * spot_prices[k] for k, v in wallet_.items()}.values())

    symb_2_bin_wallet = {k: sum(v) for k, v in wallet_lst.items()}
    symb_2_urlz_pnl = {k: sum(v) for k, v in urlz_lst.items()}
    symb_2_urlz_pnl_usd = {'USDT': sum(symb_2_urlz_pnl.values())}
    symb_2_futs = {k[:-4]: sum(v) for k, v in futs_lst.items()}
    
    
    ################################# init contracts
    #################################
    
    # contracts commented out
    arbi_contracts_yes = [               
    ]
    
    eth_contracts_yes = [
    ]

    contracts_yes = {
        'ARBI': arbi_contracts_yes,
        'ETH': eth_contracts_yes,
    }

    cmap = {}
    for network in ['ARBI', 'ETH']:
        cmap[network] = {}
        for symb_, contract_ in contracts.ADDRESS_BOOK[network].items():
            if symb_ in contracts_yes[network]:

                # point abi to proxy
                if contract_.address in contracts.ADDRESS_2_PROXY:
                    address_abi_ = contracts.ADDRESS_2_PROXY[contract_.address]
                else:
                    address_abi_ = None

                # create contract
                c_ = contracts.CONTRACT_FACTORIES[network].create_contract(
                    contract_.address, 
                    contract_.is_token, 
                    key_2_get_abi=contracts.NETWORK_2_API_KEY[network],
                    proxy_contract_address=address_abi_,
                )
                cmap[network][symb_] = c_
                
                
    ################################# Pending Rewards
    #################################

    # CRV - PENDING
    # (in Convex)
    crv_pending_in_cvx = float(Web3.fromWei(cmap['ETH']['<REWARD_POOL>'].functions.earned(
        account=portfolio_utils.ADDRESSES['CRV_INST']).call(), unit="ether"))

    # (in Curve)
    # change abi of claimable tokens to view
    abi_tmp_ = list(cmap['ETH']['<POOL>'].abi)
    abi_tmp_[12]['stateMutability'] = 'view' 
    cmap['ETH']['<POOL>'].abi = abi_tmp_
    crv_pending_in_crv = contracts.convert_balance_2_units(
        cmap['ETH']['<POOL>'].functions.claimable_tokens(portfolio_utils.ADDRESSES['CRV_INST']).call())

    crv_pending = crv_pending_in_cvx + crv_pending_in_crv

    # GMX - PENDING
    eth_pending = 0
    esgmx_pending = 0
    for address_ in GMX_ADDRESSES:
        eth_pending += float(Web3.fromWei(cmap['ARBI']['fglp'].functions.claimable(address_).call(), unit="ether"))
        esgmx_pending += float(Web3.fromWei(cmap['ARBI']['fsglp'].functions.claimable(address_).call(), unit="ether"))

    symb_2_pending = {
        'CRV': crv_pending,
        'esGMX': esgmx_pending,
        'ETH': eth_pending,

    }
    
    ################################# balanceOf
    #################################
    
    crv_balance_of = cmap['ETH']['crv'].functions.balanceOf(portfolio_utils.ADDRESSES['CRV_INST']).call()
    gmx_balance_of = 0
    esgmx_balance_of = 0
    for address_ in GMX_ADDRESSES:
        gmx_balance_of += cmap['ARBI']['gmx'].functions.balanceOf(address_).call()
        esgmx_balance_of += cmap['ARBI']['fsglp'].functions.cumulativeRewards(address_).call()
        
    # claimed rewards
    # sell all rewards into usdc in wallets,
    # if transferred to binance, equity still picked up
    usdc_balance_of = 0
    for address_ in [
        portfolio_utils.ADDRESSES['CRV_INST'],
        ]:
        usdc_balance_of += cmap['ETH']['usdc'].functions.balanceOf(address_).call()


#     # how much CRV we'll receive if withdraw
#     # (includes slippage)
#     # 0 index = CRV
#     crv_withdraw = contracts.convert_balance_2_units(
#         cmap['ETH']['<POOL>'].functions.calc_withdraw_one_coin(int(crv_supply_lp), 0).call(),
#         unit='wei'
#     )

    symb_2_balance_of = {
        'ARB': contracts.convert_balance_2_units(arb_balance_of, unit="ether"),
        'esGMX': contracts.convert_balance_2_units(esgmx_balance_of, unit="ether"),
        'USDC': contracts.convert_balance_2_units(usdc_balance_of, unit="mwei"),
        # 'CRV': crv_withdraw, # includes slippage
    }
    
    ################################# Glp Equity
    #################################

    glp_native = defaultdict(list)
    glp_trader_bias = defaultdict(list)
    glp_tokens = 0

    for address_ in GMX_ADDRESSES:
        g = gmx_utils.GmxRisk(address_, 'ARBI', cmap, logging=False)
        out = g.main()
        glp_risk_native_ = out['glp_native_risk']
        glp_risk_ = out['glp_risk']
        trader_bias_net_ = out['trader_bias']

        # 
        [glp_native[k].append(v) for k, v in glp_risk_native_.items()]
        [glp_trader_bias[k].append(v) for k, v in trader_bias_net_.items()]
        glp_tokens += glp_risk_

    symb_2_glp_native = {k: sum(v) for k, v in glp_native.items()}
    symb_2_trader_bias = {k: sum(v) for k, v in glp_trader_bias.items()}
    
    ################################# Curve Pool price risk
    #################################
    
    _, symb_2_crv_pool_risk = crv_utils.calc_risk_of_crv_tricrypto(portfolio_utils.ADDRESSES['CRV_INST'])
    symb_2_crv_pool_risk.pop('USD')
    

    ################################# Assets
    #################################
    
    def convert_index_keys_2_native(ser):
        s = pd.Series(ser)
        s.index = [gmx_utils.WRAPPERS_2_NATIVE.get(i, i) for i in s.index]
        return s

    assets_native = pd.concat([
        convert_index_keys_2_native(pd.Series(symb_2_bin_wallet)),
        convert_index_keys_2_native(pd.Series(symb_2_urlz_pnl_usd)),
        convert_index_keys_2_native(pd.Series(symb_2_balance_of)),
        convert_index_keys_2_native(pd.Series(symb_2_pending)),
        convert_index_keys_2_native(pd.Series(symb_2_glp_native)),
    ], axis=1, keys=['bin_wallet', 'bin_urlz_pnl', 'balance_of', 'pending', 'glp_native'])


    ################################# Portfolio Equity
    #################################
    
    asset_tmp = defaultdict(list)
    for d_ in [
        symb_2_bin_wallet, 
        symb_2_urlz_pnl_usd, 
        symb_2_balance_of, 
        symb_2_pending, 
        symb_2_glp_native]:

        [asset_tmp[gmx_utils.WRAPPERS_2_NATIVE.get(k, k)].append(v) for k, v in d_.items()]

    symb_2_asset = {k: sum(v) for k, v in asset_tmp.items()}      
    symb_2_asset_usd = sum({k: v * spot_prices[k] for k, v in symb_2_asset.items()}.values())

    # loans native
    # loans dollar
    LOANS = config.LOANS
    loans_lst = defaultdict(list)
    for _, d in LOANS.items():
        tmp = {k: sum(v.values()) for k, v in d.items()}
        [loans_lst[k_].append(v_) for k_, v_ in tmp.items()]
    symb_2_loans = {k: sum(v) for k, v in loans_lst.items()}
    symb_2_loans_usd = sum({k: v * spot_prices[k] for k, v in symb_2_loans.items()}.values())

    # portfolio equity $
    equity_all_usd = symb_2_asset_usd - symb_2_loans_usd

    
    ################################# Portfolio Price Risk
    #################################
    
    # account for urlz pnl as native risk
    symb_2_urlz_tmp = {'USDT': 0}
    for symb_, urlz_usd_ in symb_2_urlz_pnl.items():
        if symb_ in {'BTCUSDT', 'ETHUSDT'}:
            symb_2_urlz_tmp[symb_[:-4]] = urlz_usd_ / spot_prices[symb_[:-4]]
        else:
            symb_2_urlz_tmp['USDT'] += urlz_usd_
                
    price_risk_dict = {
        'Bin_Wallet': symb_2_bin_wallet,
        'Balance_Of': symb_2_balance_of,
        'Pending': symb_2_pending,
        'GLP': symb_2_glp_native,
        'Urlz': symb_2_urlz_tmp,
        'Futs': symb_2_futs,
        'Trader_bias': symb_2_trader_bias,
        'Loans': {k: -1 * v for k, v in symb_2_loans.items()},
    }
    price_risk_dict = {k: {gmx_utils.WRAPPERS_2_NATIVE.get(kk, kk): vv for kk, vv in d.items()} 
     for k, d in price_risk_dict.items()}
    price_risk_tmp = pd.DataFrame(price_risk_dict)
    price_risk_tmp['Net_Risk'] = price_risk_tmp.sum(axis=1)
    price_risk_tmp['Net_Risk_Usd'] = pd.Series({symb: val * spot_prices[symb] for symb, val in price_risk_tmp['Net_Risk'].items()}).round()
        
    ################################# Portfolio Alerting
    #################################
    
    price_risk_ = price_risk_tmp.T.drop(['USDT','BUSD','USDC','DAI','FRAX','USD'],axis=1).round(2).to_dict()
    price_risk_native_ = {}
    for k_, d_ in price_risk_.items():
        d_.pop('Net_Risk')
        d_.pop('Net_Risk_Usd')
        price_risk_native_[k_] = round(sum([i for i in d_.values() if i == i]), 2)
    price_risk_usd_ = {k: int(v * spot_prices[k]) for k, v in price_risk_native_.items()}

    # logging
    logger.info(f"assets_native: {json.dumps((assets_native.to_dict()), indent=4)}")
    logger.info(f"symb_2_price_risk_detail: {json.dumps(({s:{k: round(v,2) if v==v else 0 for k,v in d.items()} for s, d in price_risk_.items()}), indent=4)}")
    logger.info(f"symb_2_price_risk: {json.dumps(({k: round(v, 2) for k, v in price_risk_native_.items()}), indent=4)}")
        

    #####Check if netRiskUsd >= $100k and send alert######
    net_risk_usd_ = sum({k: v * spot_prices[k] for k, v in price_risk_native_.items()}.values())
    if abs(net_risk_usd_) >= 100000:
        msg = f"netRiskUsd: {int(net_risk_usd_)}"
        url = f'https://api.telegram.org/bot{config.TG_BOT_TOKEN}/sendMessage?chat_id={config.TG_GMX_PORTFOLIO_CHAT_ID}&parse_1mode=markdown&text={msg}' 
        requests.get(url)
        
        
    ################################# Risk Hedger
    #################################
    class Hedger:
        def __init__(self, client):
            self.client = client

    MIN_AVAIL_BAL_USD = 150000
        
    # combine escrowed and native risks
    price_risk_usd_adj = dict(price_risk_usd_)
    price_risk_usd_adj['GMX'] = price_risk_usd_['GMX'] + price_risk_usd_['esGMX']
    price_risk_usd_adj.pop('esGMX')
    
    # check past orders
    # (most recent order is last)
    orders = bgs[0].make_signed_request("GET", "userTrades")
    df_orders = pd.DataFrame(orders)
    # df_orders['time_dt'] = df_orders['time'].apply(utils.convert_ms_to_dt)
    df_orders.set_index('symbol', inplace=True)
    
    # logging
    logger.info(f"config.HEDGE_SYMBS: {json.dumps(config.HEDGE_SYMBS, indent=4)}")
    logger.info(f"config.SYMB_2_MIN_AMOUNT_MINUTES_BETWEEN_TRADE: {json.dumps(config.SYMB_2_MIN_AMOUNT_MINUTES_BETWEEN_TRADE, indent=4)}")
    logger.info(f"config.SYMB_2_MIN_AMOUNT_VOLUME_USD_1_HR: {json.dumps(config.SYMB_2_MIN_AMOUNT_VOLUME_USD_1_HR, indent=4)}")
    logger.info(f"symb_2_price_risk_$: {json.dumps(({k: int(v) for k, v in price_risk_usd_adj.items()}), indent=4)}")
    logger.info(f"equity_all_$: ${int(equity_all_usd)}")

    # client
    h = Hedger(bgs[0])s
    account = h.client.get_account()
    available_bal = float(account['availableBalance'])

    if available_bal >= MIN_AVAIL_BAL_USD:
        risk_2_hedge = {symb: risk_usd for symb, risk_usd in price_risk_usd_adj.items() 
                        if (config.HEDGE_SYMBS.get(symb, False))
                        and (abs(risk_usd) >= config.MIN_HEDGE_AMOUNT_USD[symb])}
        logger.info(f"risk_2_hedge: {json.dumps(risk_2_hedge, indent=4)}")
        
        for symb_, symb_risk_usd_ in risk_2_hedge.items():

            ########## cancel any open orders
            h.client.cancel_open_orders(f'{symb_}USDT')
            
            ########## implement trading checks
            #   (check past orders)
            # symb_minute_limit_ok = True
            # symb_volume_limit_ok = True
            symb_orders = df_orders[df_orders.index == f'{symb_}USDT']
            
            if not(symb_orders.empty):
                # condition 1: check last trade timestamp 
                symb_orders_last = symb_orders.iloc[-1, :]
                symb_orders_last_ts = symb_orders_last['time']
                dt_now = datetime.now()
                ts_now = utils.convert_dt_to_ms(dt_now.year, dt_now.month, dt_now.day)
                # minutes since last trade
                # ms / 1000 = s / 60 = m
                min_since_last_trade = int((ts_now - symb_orders_last_ts) / (1000 * 60))
                if min_since_last_trade < config.SYMB_2_MIN_AMOUNT_MINUTES_BETWEEN_TRADE[symb_]:
                    # symb_minute_limit_ok = False
                    logger.info(f"NOK: Only {min_since_last_trade}mins elapsed since last {symb_} trade")
                    break

                # condition 2: check all orders over last 1 hour
                ts_1_hr_start = ts_now - 60 * 1000
                symb_orders_last_hr = symb_orders[symb_orders['time'] >= ts_1_hr_start]
                if not(symb_orders_last_hr.empty):
                    volume_usd_1_hr = float(symb_orders_last_hr['quoteQty'].sum())
                    if volume_usd_1_hr >= config.SYMB_2_MIN_AMOUNT_VOLUME_USD_1_HR[symb_]:
                        # symb_volume_limit_ok = False
                        logger.info(f"NOK: ${volume_usd_1_hr} volume for {symb_} over last hour")
                        break
                        
            

            ########## check book
            book = h.client.get_order_book(f'{symb_}USDT')
            best_bid = float(book['bids'][0][0])
            best_ask = float(book['asks'][0][0])
            # get quantity and round to tick size
            min_symb_risk_usd = min(abs(symb_risk_usd_), config.MAX_HEDGE_AMOUNT_USD[symb_])
            qty_raw = min_symb_risk_usd / ((best_bid + best_ask) / 2)
            qty_tick = round(qty_raw, h.client.qty_tick_size[symb_])
            # if qty_tick is 0, set to int
            if not(h.client.qty_tick_size[symb_]):
                qty_tick = int(qty_tick)

            ########## place limits with accepable slippage 
            bid_w_slippage = best_bid - best_bid * config.MAX_SLIPPAGE_PCT[symb_]
            ask_w_slippage = best_ask + best_ask * config.MAX_SLIPPAGE_PCT[symb_]
            price_raw = ask_w_slippage if symb_risk_usd_ < 0 else bid_w_slippage
            price_tick = round(price_raw, h.client.price_tick_size[symb_])

            ########## post order
            order_params = {
                'symbol': f'{symb_}USDT',
                'side': 'BUY' if symb_risk_usd_ < 0 else 'SELL',
                'type_': 'LIMIT',
                'quantity': qty_tick,
                'price': price_tick,
                'timeInForce': 'GTC',
            }
            h.client.post_order(**order_params)
            logger.info(f"ORDER POSTED: {order_params}")
    else:
        logger.info(f"NOK; Available Balance {available_bal} < MIN_AVAIL_BAL_USD {MIN_AVAIL_BAL_USD}, no trades placed")