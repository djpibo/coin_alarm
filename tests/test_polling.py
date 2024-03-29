import asyncio
from datetime import datetime, timedelta

import pymongo
import pytz
import redis
import requests
from discord_webhook import DiscordWebhook, DiscordEmbed

from batch.polling import get_coin_lists, api_failure, url_candles, candle_result_v1
from pymongo import MongoClient

from connection.postgres import get_by_id, CodeAlarm
from dao.postgres import get_data_by_id

headers = {"accept": "application/json"}
url_markets = "https://api.upbit.com/v1/market/all?isDetails=true"

r = redis.Redis(host='localhost', port=6379, db=0)
markets = []

client = MongoClient(host='localhost', port=27017)
db = client['test_database']
collection = db['test_collection']
ticker_result = db['ticker_result']


def test_get_coin_lists():
    response = requests.get(url_markets, headers=headers)
    coin_lists = response.json()
    r.delete("KRW_coins")
    _data = filter(lambda x: x['market'].startswith('KRW'), coin_lists)
    r.lpush("KRW_coins", *[item['market'] for item in _data])
    print(f"get_coin_list called. {r.lrange("KRW_coins", 0, -1)}")
    print(r.llen("KRW_coins"))


def test_set_params() -> markets:
    data = r.lrange("KRW_coins", 0, -1)
    if not data:
        get_coin_lists()
    list_values_strings = [value.decode('utf-8') for value in r.lrange("KRW_coins", 0, -1)]
    print(list_values_strings)
    return r.lrange("KRW_coins", 0, -1)


def test_percentage_alarm():
    for dynamic_market in [value.decode('utf-8') for value in r.lrange("KRW_coins", 0, -1)]:
        _result = collection.find({"market": dynamic_market}).sort("timestamp", -1).limit(2)
        gap = abs(_result[0].get("change_rate") - _result[1].get("change_rate"))
        print(gap)


def test_candle():
    curr_time = date_formatter(0)
    prev_time = date_formatter(9)
    for value in r.lrange("KRW_coins", 0, -1):
        getAndSaveCurrCandle(candle_result_v1, value.decode('utf-8'), curr_time)
        getAndSaveCurrCandle(candle_result_v1, value.decode('utf-8'), prev_time)


def getAndSaveCurrCandle(candles_result, _market: str, _time: str):
    _response = requests.get(
        f"https://api.upbit.com/v1/candles/minutes/3?count=180&market={_market}&to={_time}"
        , headers=headers)
    _json = _response.json()
    if _json:
        if _response.status_code == 200:
            # Perform bulk insert
            bulk_operations = [pymongo.InsertOne(doc) for doc in _json]
            result = candles_result.bulk_write(bulk_operations, ordered=False)
            print(result)
        else:
            print(f"ERROR {_json}")
    else:
        print(f"ERROR {_market}")


def test_any():
    # print(date_formatter(9))
    test_discord_alarm("DCR001", "example")


def date_formatter(hour: int) -> str:
    # Get the current time
    current_time = datetime.now()  # pytz.timezone('Asia/Tokyo')
    # Set minute and second to 0
    current_time = current_time.replace(minute=0, second=0)
    # Subtract 9 hours from the current time
    nine_hours_ago = current_time - timedelta(hours=hour)
    # Format the datetime using ISO 8601 format
    formatted_time = nine_hours_ago.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Replace ":" with "%3A" and "+" with "%2B"
    return formatted_time.replace(":", "%3A")


def test_discord_alarm(_id: str, content: str):
    data: CodeAlarm = get_by_id(_id)
    webhook = DiscordWebhook(url=data.discord_webhook_url, thread_id=data.thread_id)
    embed = DiscordEmbed(title=data.title, description=content, color="03b2f8")
    webhook.add_embed(embed)
    webhook.execute()
