# coding: utf-8

from multiprocessing import Pool
from huobi_websocket import GetHuoBiData
from binance_websocket import GetBinanceData
from gdax_websocket import GetGdaxData


def run_websocket(func):
    func.run()


if __name__ == '__main__':
    task_list = [
        GetHuoBiData(), GetBinanceData(), GetGdaxData()
    ]
    with Pool(3) as process_pool:
        process_pool.map(run_websocket, task_list)
