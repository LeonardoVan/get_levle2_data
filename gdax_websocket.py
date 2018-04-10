# coding: utf-8

# import sys
import time
import datetime
import pymongo
import config
from gdax.order_book import OrderBook


class OrderBookConsole(OrderBook):
    ''' Logs real-time changes to the bid-ask spread to the console '''

    def __init__(self, product_id=['BTC-USD', 'ETH-USD']):
        super(OrderBookConsole, self).__init__(product_id=product_id)

        # latest values of bid-ask spread
        # self._bid = None
        # self._ask = None
        # self._bid_depth = None
        # self._ask_depth = None
        self.uri = config.mongo_uri

    def save(self, data):
        with pymongo.MongoClient(self.uri) as conn:
            u = {'date': data['date'], 'type': data['type'], 'orderId': data['orderId']}
            conn.nowdone.gdax_tick_data.update(u, data, upsert=True)

    def on_message(self, message):
        # print(message)
        # super(OrderBookConsole, self).on_message(message)
        if 'price' in message and 'type' in message:
            # if message['type'] == 'received':
            #     size = float(message['size'])
            #     remaining_size = None
            # else:
            #     size = None
            #     remaining_size = float(message['remaining_size'])
            message_dict = {
                'type': message['type'],
                'side': message['side'],
                'orderId': message['order_id'] if 'order_id' in message else None,
                'productId': message['product_id'],
                'price': float(message['price']),
                'remainingSize': float(message['remaining_size']) if 'remaining_size' in message else None,
                'size': float(message['size']) if 'size' in message else None,
                'sequence': message['sequence'],
                'date': datetime.datetime.strptime(message['time'].replace('Z', ''), '%Y-%m-%dT%H:%M:%S.%f'),
                'clientId': message['client_id'] if 'client_id' in message else None,
                'orderType': message['order_type'] if 'order_type' in message else None,
                'reason': message['reason'] if 'reason' in message else None,
                'tradeId': message['trade_id'] if 'trade_id' in message else None,
                'makerOrderId': message['maker_order_id'] if 'maker_order_id' in message else None,
                'takerOrderId': message['taker_order_id'] if 'taker_order_id' in message else None,
            }
            # print(message_dict)
            self.save(message_dict)
            """
            # Calculate newest bid-ask spread
            bid = self.get_bid()
            bids = self.get_bids(bid)
            bid_depth = sum([b['size'] for b in bids])
            ask = self.get_ask()
            asks = self.get_asks(ask)
            ask_depth = sum([a['size'] for a in asks])

            if self._bid == bid and self._ask == ask and self._bid_depth == bid_depth and self._ask_depth == ask_depth:
                # If there are no changes to the bid-ask spread since the last update, no need to print
                pass
            else:
                # If there are differences, update the cache
                self._bid = bid
                self._ask = ask
                self._bid_depth = bid_depth
                self._ask_depth = ask_depth
                print('{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                    dt.datetime.now(), self.product_id, bid_depth, bid, ask_depth, ask))
            """


class GetGdaxData(object):
    def __init__(self):
        self.order_book = OrderBookConsole()

    def run(self):
        self.order_book.start()
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            self.order_book.close()

        # if self.order_book.error:
        #     sys.exit(1)
        # else:
        #     sys.exit(0)


if __name__ == '__main__':
    get_gdax = GetGdaxData()
    get_gdax.run()
