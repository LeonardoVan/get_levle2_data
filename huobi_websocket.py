# coding: utf-8

from websocket import create_connection
import gzip
import time
import datetime
import pymongo
import json
import config


class GetHuoBiData(object):
    def __init__(self):
        self.uri = config.mongo_uri

    def save(self, data):
        with pymongo.MongoClient(self.uri) as conn:
            u = {'channel': data['channel'], 'channelUpdateTime': data['channelUpdateTime']}
            conn.nowdone.huobi_tick_data.update(u, data, upsert=True)

    def run(self):
        while(1):
            try:
                ws = create_connection("wss://api.huobipro.com/ws")
                break
            except (Exception) as e:
                print(e)
                print('connect ws error,retry...')
                time.sleep(2)

        # 订阅 Trade Detail 数据
        btc_trade_str = """{"sub": "market.btcusdt.trade.detail", "id": "id10"}"""
        eth_trade_str = """{"sub": "market.ethusdt.trade.detail", "id": "id10"}"""

        ws.send(btc_trade_str)
        ws.send(eth_trade_str)
        while(1):
            compressData = ws.recv()
            result = gzip.decompress(compressData).decode('utf-8')
            if result[:7] == '{"ping"':
                ts = result[8:21]
                pong = '{"pong":'+ts+'}'
                ws.send(pong)
                ws.send(btc_trade_str)
                ws.send(eth_trade_str)
            else:
                result = json.loads(result)
                if 'ch' in result.keys():
                    channel = result['ch']
                    channel_time = datetime.datetime.utcfromtimestamp(result['ts']/1000)
                    _product_id = channel.split('.')[1]
                    product_id = '{}-{}'.format(_product_id[:3].upper(), _product_id[3:].upper())
                    _data = [{
                        'direction': d['direction'], 'amount': float(d['amount']),
                        'tradeTime': datetime.datetime.utcfromtimestamp(d['ts']/1000),
                        'tradeId': str(d['id']), 'price': float(d['price'])
                    } for d in result['tick']['data']]
                    tick_id = str(result['tick']['id'])
                    tick_time = datetime.datetime.utcfromtimestamp(result['tick']['ts']/1000)
                    _dict = {
                        'channel': channel, 'channelUpdateTime': channel_time,
                        'productId': product_id,
                        'tick': {
                            'tickId': tick_id, 'tickTime': tick_time, 'data': _data
                        }
                    }
                    self.save(_dict)
                else:
                    pass


if __name__ == '__main__':
    get_huobi = GetHuoBiData()
    get_huobi.run()
