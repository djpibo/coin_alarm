# What should I do ?
# 1. Alarm : 1% increase
# 2. Alarm : Record High
import asyncio
import json
# memo
# [] is dict, {} is list

import time
from datetime import datetime

import redis
import requests

from pymongo import MongoClient
from discord_webhook import DiscordWebhook, DiscordEmbed

r = redis.Redis(host='localhost', port=6379, db=0)
client = MongoClient(host='localhost', port=27017)
db = client['test_database']
collection = db['test_collection']
ticker_result = db['ticker_result']
api_failure = db['api_failure']
rise_list = db['rise_list']
down_list = db['down_list']

headers = {"accept": "application/json"}
coin_up_list = []
coin_down_list = []
record_highest_list = []
record_lowest_list = []

url_markets = "https://api.upbit.com/v1/market/all?isDetails=true"
url_ticker = "https://api.upbit.com/v1/ticker"
url_data = "https://api.upbit.com/v1/market/all?isDetails=true"
url_candle = "https://api.upbit.com/v1/candles/minutes/1?market=KRW-BTC&count=1"
url_trade = "https://api.upbit.com/v1/trades/ticks?market=KRW-BTC&count=1&daysAgo=7"


def upbit_tracker():
    _response = requests.get(
        url_ticker
        , params={"markets": [value.decode('utf-8') for value in r.lrange("KRW_coins", 0, -1)]}
        , headers=headers)
    _json = _response.json()
    print(f"mongoDB insert : "
          f"{ticker_result.insert_many(_json) if _response.status_code == 200 else api_failure.insert_many(_json)}")


def alarm():
    if coin_up_list:
        print(f"상승폭 알람 : {coin_up_list}")
        discord_alarm(f"상승폭 알람", f"대상 코인 : {coin_up_list}")
        coin_up_list.clear()

    if coin_down_list:
        print(f"하락폭 알람 : {coin_down_list}")
        discord_alarm(f"하락폭 알람", f"대상 코인 : {coin_down_list}")

    if record_highest_list:
        print(f"최고치 알람 : {record_highest_list}")
        discord_alarm(f"최고치 알람", f"대상 코인 : {record_highest_list}")

    if record_lowest_list:
        print(f"최저치 알람 : {record_lowest_list}")
        discord_alarm(f"최저치 알람", f"대상 코인 : {record_lowest_list}")


def coin_perform():
    total = 0
    count = r.llen("KRW_coins")
    for dynamic_market in [value.decode('utf-8') for value in r.lrange("KRW_coins", 0, -1)]:
        _result = collection.find({"market": dynamic_market}).sort("timestamp", -1).limit(2)
        gap = abs(_result[0].get("change_rate") - _result[1].get("change_rate"))
        save_record(dynamic_market, _result[0].get("trade_price"))
        total = _result[0].get("change_rate") * 100
        if gap > 0.01:
            if _result[0].get("change") == "RISE":
                save_gap_list(_result[0], gap, 1)
                coin_up_list.append(dynamic_market)
            else:
                save_gap_list(_result[0], gap, 0)
                coin_down_list.append(dynamic_market)
    discord_alarm(f"평균 변화율 알람", f"대상 코인 건수 : {count}, 평균 변화율 : {format(total / count, ',.4f')}%")


def get_coin_lists():
    response = requests.get(url_markets, headers=headers)
    coin_lists = response.json()
    _data = filter(lambda x: x['market'].startswith('KRW'), coin_lists)
    r.delete("KRW_coins")
    r.lpush("KRW_coins", *[item['market'] for item in _data])
    print(f"get_coin_list called. {r.lrange("KRW_coins", 0, -1)}")


def save_gap_list(result_curr, gap, flag: int):
    data = {"market": result_curr.get("market"),
            "gap": format(gap * 100, ".2f"),
            "trade_date": result_curr.get("trade_date"),
            "trade_time": result_curr.get("trade_time_kst"),
            "change": result_curr.get("change"),
            "change_rate": result_curr.get("change_rate")}
    if flag == 1:
        rise_list.insert_one(data)
    else:
        down_list.insert_one(data)


def save_record(coin_name: str, trade_price: float):
    coin_count = r.hget(f"{coin_name}", 'count')
    if not coin_count:
        coin_count = 0
        r.hset(f"{coin_name}", 'highest', str(trade_price))
        r.hset(f"{coin_name}", 'lowest', str(trade_price))
        r.hset(f"{coin_name}", 'curr_trade_date', datetime.now().strftime("%Y%m%d"))
        r.hset(f"{coin_name}", 'curr_trade_time', datetime.now().strftime("%Y%m%d"))
        r.hset(f"{coin_name}", 'prev_trade_date', '0')
        r.hset(f"{coin_name}", 'prev_trade_time', '0')
        r.hset(f"{coin_name}", 'count', '0')

    highest_price = float(r.hget(f"{coin_name}", 'highest'))
    lowest_price = float(r.hget(f"{coin_name}", 'lowest'))

    if int(coin_count) > 5:
        if trade_price > highest_price:
            highest_price = trade_price
            record_highest_list.append(coin_name)
        if trade_price < lowest_price:
            lowest_price = trade_price
            record_lowest_list.append(coin_name)
        r.hset(f"{coin_name}", 'highest', str(highest_price))
        r.hset(f"{coin_name}", 'lowest', str(lowest_price))
        r.hset(f"{coin_name}", 'curr_trade_date', datetime.now().strftime("%Y%m%d"))
        r.hset(f"{coin_name}", 'curr_trade_time', datetime.now().strftime("%Y%m%d"))
        r.hset(f"{coin_name}", 'prev_trade_date', r.hget(f"{coin_name}", 'curr_trade_date'))
        r.hset(f"{coin_name}", 'prev_trade_time', r.hget(f"{coin_name}", 'curr_trade_time'))
        r.hset(f"{coin_name}", 'count', '0')

    r.hincrby(f"{coin_name}", "count", 1)


def discord_alarm(title: str, content: str):
    list_data = r.lrange('discord_webhook_url', 0, -1)[0]
    webhook = DiscordWebhook(url=list_data)
    embed = DiscordEmbed(title=title, description=content, color="03b2f8")
    webhook.add_embed(embed)
    webhook.execute()
    # webhook = DiscordWebhook(url=list_data, thread_name="test")
    # embed = DiscordEmbed(title=title, description=content, color="03b2f8")
    # webhook.add_embed(embed)
    # webhook.execute()


async def polling_executor_1():
    while True:
        upbit_tracker()
        coin_perform()
        alarm()
        await asyncio.sleep(60 * 1)


async def polling_executor_5():
    while True:
        get_coin_lists()
        await asyncio.sleep(60 * 5)


async def main():
    await polling_executor_1()
    await polling_executor_5()

if __name__ == "__main__":
    asyncio.run(main())