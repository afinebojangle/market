from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class EquityHistorical(Base):
    __tablename__ = 'equity_historical'
    ticker = Column(String(10), primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    ex_dividend = Column(Float, nullable=False)
    split_ratio = Column(Float, nullable=False)
    adj_open = Column(Float, nullable=False)
    adj_high = Column(Float, nullable=False)
    adj_low = Column(Float, nullable=False)
    adj_close = Column(Float, nullable=False)
    adj_volume = Column(Integer, nullable=False)

Base.metadata.create_all(engine)
