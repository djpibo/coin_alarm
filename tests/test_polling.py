import unittest

import redis
import requests

from batch.polling import get_coin_lists
from pymongo import MongoClient

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
