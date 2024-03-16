from pydantic import BaseModel


class Candle(BaseModel):
    market: str
    candle_date_time_utc: str
    candle_date_time_kst: str
    opening_price: float
    high_price: float
    low_price: float
    trade_price: float
    timestamp: int
    candle_acc_trade_price: float
    candle_acc_trade_volume: float
    unit: int
