# What should I do ?
# 1. Alarm : 1% increase
# 2. Alarm : Record High

# memo
# [] is dict, {} is list

import time
from datetime import datetime

import redis
import requests

from pymongo import MongoClient
from win10toast import ToastNotifier

toaster = ToastNotifier()
r = redis.Redis(host='localhost', port=6379, db=0)
client = MongoClient(host='localhost', port=27017)
db = client['test_database']
collection = db['test_collection']
rise_list = db['rise_list']
down_list = db['down_list']

headers = {"accept": "application/json"}
params = {}
markets = []
coin_up_list = []
coin_down_list = []
record_highest_list = []
record_lowest_list = []
total = 0

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
        print(data_candle)
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
    for item in keys:
        dynamic_market = r.get(item).decode("UTF-8")
        # 쿼리 작성
        query = {
            "market": dynamic_market
        }
        # 가장 최근 trade_time을 찾기 위해 정렬
        sort_order = [("timestamp", -1)]
        result_prev = collection.find(query, sort=sort_order).skip(1).limit(1)[0]  # return Cursor to dict (first)
        result_curr = collection.find_one(query, sort=sort_order)  # <class 'dict'>
        save_record(dynamic_market, result_curr.get("trade_price"))
        # explain_result = result_prev.explain()
        # print(explain_result)  # This will provide details about how the query is executed, including index usage
        gap = result_curr.get("change_rate") - result_prev.get("change_rate")
        if gap > 0.01:
            if result_curr.get("change") == "RISE":
                save_gap_list(result_curr, gap, 1)
                coin_up_list.append(dynamic_market)
            else:
                save_gap_list(result_curr, gap, 0)
                coin_down_list.append(dynamic_market)

    if coin_up_list:
        print(coin_up_list)
        toaster.show_toast(f"상승폭 1% 이상 알림", f"대상 코인 : {coin_up_list}", duration=10)
    coin_up_list.clear()

    if coin_down_list:
        print(coin_down_list)
        toaster.show_toast(f"하락폭 1% 이상 알림", f"대상 코인 : {coin_down_list}", duration=10)
    coin_down_list.clear()

    if record_highest_list:
        print(record_highest_list)
        toaster.show_toast(f"최고치 갱신", f"대상 코인 : {record_highest_list}", duration=5)
    record_highest_list.clear()

    if record_lowest_list:
        print(record_lowest_list)
        toaster.show_toast(f"최저치 갱신", f"대상 코인 : {record_lowest_list}", duration=5)
    record_lowest_list.clear()


def get_coin_lists():
    response = requests.get(url_markets, headers=headers)
    coin_lists = response.json()
    for item in coin_lists:
        market = item["market"]
        r.set(f"market:{market}", market)


def save_gap_list(result_curr, gap, flag: int):
    data = {"market": result_curr.get("market"),
            "gap": format(gap*100, ".2f"),
            "trade_date": result_curr.get("trade_date"),
            "trade_time": result_curr.get("trade_time_kst"),
            "change": result_curr.get("change"),
            "change_rate": result_curr.get("change_rate")}
    if flag == 1:
        rise_list.insert_one(data)
    else:
        down_list.insert_one(data)


def save_record(coin_name: str, trade_price: float):
    r.zadd(f"count:{coin_name}", {'count': 0}, True)  # only for create, dismiss update
    coin_info = r.hgetall(f"{coin_name}")
    if not coin_info:
        coin_data = {
            'highest': trade_price,
            'lowest': trade_price,
            'trade_date': datetime.now().strftime("%Y%m%d"),
            'trade_time': datetime.now().strftime("%H%M%S"),
        }
        for field, value in coin_data.items():
            r.hset(f"{coin_name}", field, str(value))

    if int(r.zscore(f"count:{coin_name}", 'count')) > 3:
        if trade_price > float(r.hget(f"{coin_name}", 'highest')):
            r.hset(f"{coin_name}", 'highest', str(trade_price))
            record_highest_list.append(coin_name)
        if trade_price < float(r.hget(f"{coin_name}", 'lowest')):
            r.hset(f"{coin_name}", 'lowest', str(trade_price))
            record_lowest_list.append(coin_name)
        r.hset(f"{coin_name}", 'trade_date', datetime.now().strftime("%Y%m%d"))
        r.hset(f"{coin_name}", 'trade_time', datetime.now().strftime("%H%M%S"))
        r.zadd(f"count:{coin_name}", {'count': 0}, False, True)

    r.zincrby(f"count:{coin_name}", 1, 'count')


def main():
    while True:
        call_api()
        time.sleep(60 * 1)


if __name__ == "__main__":
    main()


