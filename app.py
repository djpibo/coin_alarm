# What should I do ?
# 1. Alarm : 2% increase
# 2. Alarm : Record High

# memo
# [] is dict, {} is list

import time
import redis
import requests

from pymongo import MongoClient
from win10toast import ToastNotifier

toaster = ToastNotifier()
r = redis.Redis(host='localhost', port=6379, db=0)
client = MongoClient(host='localhost', port=27017)
db = client['test_database']
collection = db['test_collection']

headers = {"accept": "application/json"}
params = {}
markets = []

url_markets = "https://api.upbit.com/v1/market/all?isDetails=true"
url_ticker = "https://api.upbit.com/v1/ticker"
url_data = "https://api.upbit.com/v1/market/all?isDetails=true"
url_candle = "https://api.upbit.com/v1/candles/minutes/1?market=KRW-BTC&count=1"
url_trade = "https://api.upbit.com/v1/trades/ticks?market=KRW-BTC&count=1&daysAgo=7"


def call_api():
    params["markets"] = set_params("market:KRW*")
    res_candle = requests.get(url_ticker, params=params, headers=headers)
    if res_candle.status_code == 200:
        data_candle = res_candle.json()
        print(collection.insert_many(data_candle))
        percentage_alarm("market:KRW*")
    else:
        print(res_candle.content)


def set_params(key: str) -> markets:
    keys = r.keys(key)
    if not keys:
        get_coin_lists()
        keys = r.keys(key)
    if not markets:
        for item in keys:
            markets.append(r.get(item).decode('utf-8'))
    return markets


def percentage_alarm(key: str):
    keys = r.keys(key)
    coin_list = []
    for item in keys:
        dynamic_market = r.get(item).decode("UTF-8")
        # 쿼리 작성
        query = {
            "change": 'RISE',
            "market": dynamic_market
        }
        # 가장 최근 trade_time을 찾기 위해 정렬
        sort_order = [("trade_time", -1)]
        result_prev = collection.find(query, sort=sort_order).skip(1).limit(1)
        result_curr = collection.find_one(query, sort=sort_order)
        for document in result_prev:
            gap = result_curr.get("change_rate") - document.get("change_rate")
            if gap > 0.01:
                coin_list.append(dynamic_market)
    if coin_list:
        any: toaster.show_toast(f"상승폭 1% 이상 알림", f"대상 코인 : {coin_list}", duration=10)
    coin_list.clear()


def get_coin_lists():
    response = requests.get(url_markets, headers=headers)
    coin_list = response.json()
    for item in coin_list:
        market = item["market"]
        r.set(f"market:{market}", market)


def main():
    while True:
        call_api()
        time.sleep(60 * 1)


if __name__ == "__main__":
    main()
