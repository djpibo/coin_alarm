# What should I do ?
# 1. Alarm : 1% increase
# 2. Alarm : Record High
# memo
# [] is dict, {} is list

import pymongo
import redis
import requests
import asyncio

from datetime import datetime, timedelta
from pymongo import MongoClient
from discord_webhook import DiscordWebhook, DiscordEmbed

from connection.postgres import CodeAlarm, get_by_id
from dao.postgres import get_data_by_id

# Connect to the postgresSQL database
r = redis.Redis(host='localhost', port=6379, db=0)
client = MongoClient(host='localhost', port=27017)
db = client['test_database']
c10_v = r.get("candles_result_version")
ticker_result = db['ticker_result']
candle_result_v1 = db['candle_result_v1']
candle_result_v2 = db['candle_result_v2']
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
url_trade = "https://api.upbit.com/v1/trades/ticks?market=KRW-BTC&count=1&daysAgo=7"
url_candles = "https://api.upbit.com/v1/candles/minutes/3?count=180"


async def getAndSaveCurrCandle(candles_result, _market: str, _time: str):
    _response = requests.get(
        f"https://api.upbit.com/v1/candles/minutes/3?count=180&market={_market}&to={_time}"
        , headers=headers)
    _json = _response.json()
    if _json:
        if _response.status_code == 200:
            # Perform bulk insert
            bulk_operations = [pymongo.InsertOne(doc) for doc in _json]
            candles_result.bulk_write(bulk_operations, ordered=False)
        else:
            print(f"ERROR {_json}")
    else:
        print(f"ERROR {_market}")


def date_formatter(hour: int) -> str:
    # Get the current time
    current_time = datetime.now()
    # Set minute and second to 0
    current_time = current_time.replace(minute=0, second=0)
    # Subtract 9 hours from the current time
    nine_hours_ago = current_time - timedelta(hours=hour)
    # Format the datetime using ISO 8601 format
    formatted_time = nine_hours_ago.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Replace ":" with "%3A" and "+" with "%2B"
    return formatted_time.replace(":", "%3A")


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
        discord_alarm("DCR001", f"{coin_up_list}")
        coin_up_list.clear()

    if coin_down_list:
        print(f"하락폭 알람 : {coin_down_list}")
        discord_alarm("DCR002", f"{coin_down_list}")

    if record_highest_list:
        print(f"최고치 알람 : {record_highest_list}")
        discord_alarm("DCR003", f"{record_highest_list}")

    if record_lowest_list:
        print(f"최저치 알람 : {record_lowest_list}")
        discord_alarm("DCR004", f"{record_lowest_list}")


def gap_calculator(_result, dynamic_market):
    if _result.__sizeof__() == 2:
        gap = abs(_result[0].get("change_rate") - _result[1].get("change_rate"))
        if gap > 0.01:
            if _result[0].get("change") == "RISE":
                save_gap_list(_result[0], gap, 1)
                coin_up_list.append(dynamic_market)
            else:
                save_gap_list(_result[0], gap, 0)
                coin_down_list.append(dynamic_market)


def coin_perform():
    total = 0
    count = r.llen("KRW_coins")
    for dynamic_market in [value.decode('utf-8') for value in r.lrange("KRW_coins", 0, -1)]:
        _result = ticker_result.find({"market": dynamic_market}).sort("timestamp", -1).limit(2)
        gap_calculator(_result, dynamic_market)
        save_record(dynamic_market, _result[0].get("trade_price"))
        total = _result[0].get("change_rate") * 100

    discord_alarm("DCR006", f"대상 코인 건수 : {count}, 평균 변화율 : {format(total / count, ',.4f')}%")


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


def discord_alarm(_id: str, content: str):
    data: CodeAlarm = get_by_id(_id)
    webhook = DiscordWebhook(url=data.discord_webhook_url, thread_id=data.thread_id)
    embed = DiscordEmbed(title=data.title, description=content, color="03b2f8")
    webhook.add_embed(embed)
    webhook.execute()


async def batch_candle():
    discord_alarm("DCR005", "최근 18시간 데이터 적재 배치 시작")
    if int(r.get("candles_result_version")) == 1:
        candles_result = candle_result_v2
    else:
        candles_result = candle_result_v1

    curr_time = date_formatter(0)
    prev_time = date_formatter(9)
    discord_alarm("DCR005", f"배치 작업 대상 collection version {r.get("candles_result_version")} "
                            f"적재 대상 collection : {candles_result}")

    for value in r.lrange("KRW_coins", 0, -1):
        job_curr = asyncio.create_task(getAndSaveCurrCandle(candles_result, value.decode('utf-8'), curr_time))
        await job_curr
        job_prev = asyncio.create_task(getAndSaveCurrCandle(candles_result, value.decode('utf-8'), prev_time))
        await job_prev

    if int(r.get("candles_result_version")) == 1:
        db.drop_collection(candle_result_v1)
        discord_alarm("DCR005", f"배치 작업 대상 collection version {r.get("candles_result_version")} "
                                f"삭제 대상 collection : {candle_result_v1}")
        await r.set("candles_result_version", 2)
    else:
        db.drop_collection(candle_result_v2)
        discord_alarm("DCR005", f"배치 작업 대상 collection version {r.get("candles_result_version")} "
                                f"삭제 대상 collection : {candle_result_v2}")
        await r.set("candles_result_version", 1)


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


async def cron_executor():
    while True:
        current_time = datetime.now()
        if current_time.minute == 55:
            await batch_candle()
            # Sleep for a while to avoid continuous checking
            await asyncio.sleep(60 - current_time.second)  # Sleep until the next minute starts


async def main():
    job_1 = asyncio.create_task(polling_executor_1())
    job_5 = asyncio.create_task(polling_executor_5())
    cron_job = asyncio.create_task(cron_executor())

    await job_1
    await job_5
    await cron_job


if __name__ == "__main__":
    asyncio.run(main())
