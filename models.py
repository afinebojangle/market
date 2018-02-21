from sqlalchemy import Column, Integer, String, Float, Date, BigInteger
from flask_sqlalchemy import SQLAlchemy
from app import db


class EquityHistorical(db.Model):
    __tablename__ = 'equity_historical'
    ticker = Column(String(10), primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    ex_dividend = Column(Float, nullable=False)
    split_ratio = Column(Float, nullable=False)
    adj_open = Column(Float, nullable=False)
    adj_high = Column(Float, nullable=False)
    adj_low = Column(Float, nullable=False)
    adj_close = Column(Float, nullable=False)
    adj_volume = Column(BigInteger, nullable=False)
    
    
class NasdaqGlobalEquityIndex(db.Model):
    __tablename__ = 'nasdaq_global_equity_index'
    date = Column(Date, primary_key=True)
    value = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    market_value = Column(Float, nullable=False)
    dividend_market_value = Column(Float, nullable=False)
    
    
class EquityReturns(db.Model):
    __tablename__ = 'equity_returns'
    date = Column(Date, primary_key=True)
    ticker = Column(String(10), primary_key=True)
    nominal_return = Column(Float, nullable=False)
    percent_return = Column(Float, nullable=False)

class NasdaqGlobalEquityReturns(db.Model):
    __tablename__ = 'nasdaq_global_equity_returns'
    date = Column(Date, primary_key=True)
    nominal_return = Column(Float, nullable=False)
    percent_return = Column(Float, nullable=False)
    
class CapmCoefficients(db.Model):
    __tablename__ = 'capm_coefficients'
    ticker = Column(String(10), primary_key=True)
    alpha = Column(Float, nullable=False)
    beta = Column(Float, nullable=False)
    
class EquityErrors(db.Model):
    __tablename__ = 'equity_errors'
    ticker = Column(String(10), primary_key=True)
    date = Column(Date, primary_key=True)
    error = Column(Float, nullable=False)
    
class OptionHistorical(db.Model):
    __tablename__ = 'option_historical'
    ticker = Column(String(10), primary_key=True)
    experiation_date = Column(Date, primary_key=True)
    strike = Column(Float, primary_key=True)
    stock_price = Column(Float, nullable=False)
    years_to_expiration = Column(Float, nullable=False)
    call_volume = Column(Integer, nullable=False)
    call_open_interest = Column(Integer, nullable=False)
    put_volume = Column(Integer, nullable=False)
    put_open_interest = Column(Integer, nullable=False)
    call_bid_price = Column(Float, nullable=False)
    call_theoretical_value = Column(Float, nullable=False)
    call_ask_price = Column(Float, nullable=False)
    put_bid_price = Column(Float, nullable=False)
    put_theoretical_value = Column(Float, nullable=False)
    put_ask_price = Column(Float, nullable=False)
    call_bid_implied_volitility = Column(Float)
    call_mid_market_implied_volitility = Column(Float)
    call_ask_implied_volitility = Column(Float)
    smoothed_strike_implied_volitility = Column(Float, nullable=False)
    put_bid_implied_volitility = Column(Float)
    put_mid_market_implied_volitility = Column(Float)
    put_ask_implied_volitility = Column(Float)
    risk_free_interest_rate = Column(Float, nullable=False)
    dividend_rate = Column(Float, nullable=False)
    residual_rate_data = Column(Float, nullable=False)
    delta = Column(Float, nullable=False)
    gamma = Column(Float, nullable=False)
    theta = Column(Float, nullable=False)
    vega = Column(Float, nullable=False)
    rho = Column(Float, nullable=False)
    phi = Column(Float, nullable=False)
    driftless_theta = Column(Float, nullable=False)
    extended_volitility = Column(Float, nullable=False)
    extended_call_theoretical_price = Column(Float, nullable=False)
    extended_put_theoretical_price = Column(Float, nullable=False)
    trade_date = Column(Date, primary_key=True)

class EquityVolatilities(db.Model):
    __tablename__ = 'equity_volatilities'
    ticker = Column(String(10), primary_key=True)
    date = Column(Date, primary_key=True)
    ma_10 = Column(Float, nullable=False)
    ma_50 = Column(Float, nullable=False)
    ma_100 = Column(Float, nullable=False)
    ma_250 = Column(Float, nullable=False)
    
class OptionTrainingLabels(db.Model):
    __tablename__ = 'option_training_labels'
    ticker = Column(String(10), primary_key=True)
    experiation_date = Column(Date, primary_key=True)
    strike = Column(Float, primary_key=True)
    trade_date = Column(Date, primary_key=True)
    trade_type = Column(String(50), primary_key=True)
    label = Column(Float, nullable=False)
    
class OptionForwardPrices(db.Model):
    __tablename__ = 'option_forward_prices'
    ticker = Column(String(10), primary_key=True)
    experiation_date = Column(Date, primary_key=True)
    strike = Column(Float, primary_key=True)
    trade_date = Column(Date, primary_key=True)
    max_forward_call_bid_price = Column(Float, nullable=False)
    max_forward_call_ask_price = Column(Float, nullable=False)
    max_forward_put_bid_price = Column(Float, nullable=False)
    max_forward_put_ask_price = Column(Float, nullable=False)
