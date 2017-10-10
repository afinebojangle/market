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
    
    
class NasdaqGlobalEquityIndex(Base):
    __tablename__ = 'nasdaq_global_equity_index'
    date = Column(Date, primary_key=True)
    value = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    market_value = Column(Float, nullable=False)
    dividend_market_value = Column(Float, nullable=False)
    
    
class EquityReturns(Base):
    __tablename__ = 'equity_returns'
    date = Column(Date, primary_key=True)
    ticker = Column(String(10), primary_key=True)
    nominal_return = Column(Float, nullable=False)
    percent_return = Column(Float, nullable=False)
    
    

#Base.metadata.create_all(engine)
