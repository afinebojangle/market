from sqlalchemy import create_engine, func
import quandl
from sqlalchemy.orm import sessionmaker
from datetime import timedelta, date
from models import EquityHistorical, EquityReturns, NasdaqGlobalEquityIndex
import pandas as pd


engine = create_engine('postgresql://bojangles:apppas0!@marketdata.chew6qxftqgr.us-east-1.rds.amazonaws.com:5432/marketdata', echo=True)

Session = sessionmaker(bind=engine)

session = Session()

quandl.ApiConfig.api_key = 'iasNNCL-zjwhdxME_Jvx'


            
            
class DataWrangling():
    
    @staticmethod
    def calculate_equity_returns():
        
        tickers = []
        
        for result in session.query(EquityHistorical.ticker).distinct().all():
            tickers.append(result.ticker)
            
        for current_ticker in tickers:
            date_boundry = [date(2014, 1, 1)]
            for result in session.query(func.max(EquityReturns.date).label("date")).filter(EquityReturns.ticker == current_ticker).all():
                date_boundry.append(result.date)
            max_date = max(i for i in date_boundry if i is not None)
            query = session.query(EquityHistorical.ticker, EquityHistorical.date, EquityHistorical.close, EquityHistorical.adj_close).filter((EquityHistorical.date.between(max_date - timedelta(5), date.today())) & (EquityHistorical.ticker == current_ticker)).statement
            data = pd.read_sql(query, engine, index_col="date")
            data.sort_index(inplace=True)
            data['prior_close'] = data['adj_close'].shift(1)
            data['nominal_return'] = data['adj_close'] - data['prior_close']
            data['percent_return'] = data['nominal_return'] / data['prior_close']
            data.drop(['close', 'prior_close', 'adj_close'], inplace=True, axis=1)
            data.dropna(inplace=True)
            data.drop(data[data.index <= max_date].index, inplace=True)
            data.to_sql('equity_returns', engine, if_exists='append', chunksize=5000)
            
            
            
    @staticmethod
    def refresh_equity_data():
        scrapped_dates = []

        for value in session.query(EquityHistorical.date).distinct():
            scrapped_dates.append(value.date)
        
        date_range = []
        
        for n in range((date.today() - date(2014, 1, 1)).days):
                date_range.append(date(2014, 1, 1) + timedelta(n))
        
        for day in date_range:
            if day.weekday() > 4:
                pass
            elif day in scrapped_dates:
                pass
            else:
                data = quandl.get_table('WIKI/PRICES', date=day.strftime('%Y%m%d'))
                data.rename(columns={'ex-dividend': 'ex_dividend'}, inplace=True)
                data.dropna(inplace=True)
                data.to_sql('equity_historical', engine, if_exists='append', index=False, chunksize=5000)
                scrapped_dates.append(day)
                
                
    @staticmethod
    def refresh_nasdaq_global_equity_index_data():
        date_boundry = [date(2014, 1, 1)]
        
        for result in session.query(func.max(NasdaqGlobalEquityIndex.date).label("date")).all():
                date_boundry.append(result.date)
        
        max_date = max(i for i in date_boundry if i is not None)
        
        data = quandl.get('NASDAQOMX/NQGI', start_date = max_date.strftime('%Y%m%d'), end_date = date.today().strftime('%Y%m%d'))
        
        data.rename(columns={'Index Value': 'value', 'High': 'high', 'Low': 'low', 'Total Market Value': 'market_value', 'Dividend Market Value': 'dividend_market_value'}, inplace=True)
        
        data.to_sql('nasdaq_global_equity_index', engine, if_exists='append', index=True, chunksize=5000)
        
        
            
        
    
            
            
            
        

    
        
