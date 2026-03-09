from sqlalchemy import Column, Integer, String, Float, DateTime
from consumer.db import BASE


class CryptoMarketData(BASE):
    __tablename__ = "crypto_market_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    coin = Column(String(50), nullable=False)
    price_usd = Column(Float, nullable=False)
    source = Column(String(60), nullable=False)
