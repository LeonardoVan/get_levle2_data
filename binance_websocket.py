# coding: utf-8
import datetime
import pymongo
import config
from binance.client import Client
from binance.websockets import BinanceSocketManager


class GetBinanceData(object):

    def __init__(self):
        self.api_key = config.api_key
        self.api_secret = config.api_secret
        self.uri = config.mongo_uri

    def process_message(self, msg):
        print("message type: {}".format(msg['e']))
        if msg['e'] == 'error':
            pass
        else:
            _dict = {
                'eventType': msg['e'], 'eventTime': datetime.datetime.utcfromtimestamp(msg['E']/1000),
                'symbol': msg['s'], 'change': float(msg['p']), 'changeRatio': float(msg['P']),
                'weightedAvergePrice': float(msg['w']), 'preDayClose': float(msg['x']),
                'close': float(msg['c']), 'closeTradeQuantity': float(msg['Q']),
                'bestBidPrice': float(msg['b']), 'bestBidQuantity': float(msg['B']),
                'bestAskPrice': float(msg['a']), 'bestAskAuantity': float(msg['A']),
                'open': float(msg['o']), 'high': float(msg['h']), 'low': float(msg['l']),
                'totalTradedBaseVolume': float(msg['v']), 'totalTradedQuoteVolume': float(msg['q']),
                'statisticsOpenTime': datetime.datetime.utcfromtimestamp(msg['O']/1000),
                'statisticsCloseTime': datetime.datetime.utcfromtimestamp(msg['C']/1000),
                'firstTradeId': msg['F'], 'lastTradeId': msg['L'], 'totalTrades': msg['n']
            }
            self.save(_dict)

    def save(self, _dict):
        with pymongo.MongoClient(self.uri) as conn:
            u = {'eventTime': _dict['eventTime'], 'lastTradeId': _dict['lastTradeId'],
                 'firstTradeId': _dict['firstTradeId']}
            conn.nowdone.binance_tick_data.update(u, _dict, upsert=True)

    def run(self):
        client = Client(self.api_key, self.api_secret)
        bm = BinanceSocketManager(client)
        # 订阅tick数据频道
        btc_conn_key = bm.start_symbol_ticker_socket('BTCUSDT', self.process_message)
        eth_conn_key = bm.start_symbol_ticker_socket('ETHUSDT', self.process_message)
        # print(btc_conn_key, eth_conn_key)
        # bm.start_user_socket(process_message)
        bm.start()


if __name__ == '__main__':
    get_binance = GetBinanceData()
    get_binance.run()
