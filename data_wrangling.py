from sqlalchemy import func
import quandl
from datetime import timedelta, date
from models import EquityHistorical, EquityReturns, NasdaqGlobalEquityIndex, NasdaqGlobalEquityReturns, CapmCoefficients, EquityErrors
import pandas as pd
from sklearn import linear_model
from app import db


class DataWrangling():
    
    data_begin_date = date(2012, 12, 31)
    training_metric_boundry = date(2015, 12, 31)
    training_data_begin = date(2016, 1, 1)
    training_data_end = date(2016, 12, 31)
    
    @staticmethod
    def calculate_equity_returns():
        
        #db.session.query(EquityReturns).delete()
        #db.session.commit()
        
        tickers = []
        completed_tickers = []
        tickers_to_do = []
        
        for result in db.session.query(EquityHistorical.ticker).distinct().all():
            tickers.append(result.ticker)
            
        for result in db.session.query(EquityReturns.ticker).distinct().all():
            completed_tickers.append(result.ticker)
            
        tickers_to_do = set(tickers) - set(completed_tickers)
            
        for current_ticker in tickers_to_do:
            date_boundry = [DataWrangling.data_begin_date]
            for result in db.session.query(func.max(EquityReturns.date).label("date")).filter(EquityReturns.ticker == current_ticker).all():
                date_boundry.append(result.date)
            max_date = max(i for i in date_boundry if i is not None)
            query = db.session.query(EquityHistorical.ticker, EquityHistorical.date, EquityHistorical.close, EquityHistorical.adj_close).filter((EquityHistorical.date.between(max_date - timedelta(5), date.today())) & (EquityHistorical.ticker == current_ticker)).statement
            data = pd.read_sql(query, db.engine, index_col="date")
            data.sort_index(inplace=True)
            data['prior_close'] = data['adj_close'].shift(1)
            data['nominal_return'] = data['adj_close'] - data['prior_close']
            data['percent_return'] = data['nominal_return'] / data['prior_close']
            data.drop(['close', 'prior_close', 'adj_close'], inplace=True, axis=1)
            data.dropna(inplace=True)
            data.drop(data[data.index <= max_date].index, inplace=True)
            data.to_sql('equity_returns', db.engine, if_exists='append', chunksize=5000)
            
            
    @staticmethod
    def calculate_nasdaq_global_equity_returns():
        db.session.query(NasdaqGlobalEquityReturns).delete()
        db.session.commit()
        
        date_boundry = [DataWrangling.data_begin_date]
        
        for result in db.session.query(func.max(NasdaqGlobalEquityReturns.date).label("date")).all():
                date_boundry.append(result.date)
        
        max_date = max(i for i in date_boundry if i is not None)
        
        query = db.session.query(NasdaqGlobalEquityIndex.date, NasdaqGlobalEquityIndex.value).filter(NasdaqGlobalEquityIndex.date.between(max_date - timedelta(5), date.today())).statement
        
        data = pd.read_sql(query, db.engine, index_col="date")
        data.sort_index(inplace=True)
        data['prior_close'] = data['value'].shift(1)
        data['nominal_return'] = data['value'] - data['prior_close']
        data['percent_return'] = data['nominal_return'] / data['prior_close']
        data.drop(['value', 'prior_close'], inplace=True, axis=1)
        data.dropna(inplace=True)
        data.drop(data[data.index <= max_date].index, inplace=True)
        data.to_sql('nasdaq_global_equity_returns', db.engine, if_exists='append', chunksize=5000)
        
        
            
            
    @staticmethod
    def refresh_equity_data():
        quandl.ApiConfig.api_key = 'iasNNCL-zjwhdxME_Jvx'
        scrapped_dates = []

        for value in db.session.query(EquityHistorical.date).distinct():
            scrapped_dates.append(value.date)
        
        date_range = []
        
        for n in range((date.today() - DataWrangling.data_begin_date).days):
                date_range.append(DataWrangling.data_begin_date + timedelta(n))
                
        dates_to_do = list(set(date_range) - set(scrapped_dates))
        
        for day in dates_to_do:
            data = quandl.get_table('WIKI/PRICES', date=day.strftime('%Y%m%d'))
            data.rename(columns={'ex-dividend': 'ex_dividend'}, inplace=True)
            data.dropna(inplace=True)
            data.to_sql('equity_historical', db.engine, if_exists='append', index=False, chunksize=5000)
            
                
                
    @staticmethod
    def refresh_nasdaq_global_equity_index_data():
        quandl.ApiConfig.api_key = 'iasNNCL-zjwhdxME_Jvx'
        date_boundry = [DataWrangling.data_begin_date]
        
        for result in db.session.query(func.max(NasdaqGlobalEquityIndex.date).label("date")).all():
                date_boundry.append(result.date)
        
        max_date = max(i for i in date_boundry if i is not None)
        
        data = quandl.get('NASDAQOMX/NQGI', start_date = max_date.strftime('%Y%m%d'), end_date = date.today().strftime('%Y%m%d'))
        
        data.rename(columns={'Index Value': 'value', 'High': 'high', 'Low': 'low', 'Total Market Value': 'market_value', 'Dividend Market Value': 'dividend_market_value'}, inplace=True)
        
        data.to_sql('nasdaq_global_equity_index', db.engine, if_exists='append', index=True, chunksize=5000)
        
        
    @staticmethod
    def calculate_training_capm_coefficents():
        db.session.query(CapmCoefficients).delete()
        db.session.commit()
        
        all_tickers = []
        completed_tickers = []
        coefficients = []
        
        for result in db.session.query(EquityHistorical.ticker).filter(EquityHistorical.date <= DataWrangling.training_metric_boundry).distinct().all():
            all_tickers.append(result.ticker)
            
        for result in db.session.query(CapmCoefficients.ticker).distinct().all():
            completed_tickers.append(result.ticker)
            
        tickers_to_do = list(set(all_tickers) - set(completed_tickers))
        
        for current_ticker in tickers_to_do:
            query = db.session.query(EquityReturns.date.label("date"), EquityReturns.percent_return.label("stock"), NasdaqGlobalEquityReturns.percent_return.label("index")).join(NasdaqGlobalEquityReturns, EquityReturns.date == NasdaqGlobalEquityReturns.date).filter((EquityReturns.ticker == current_ticker) & (EquityReturns.date <= DataWrangling.training_metric_boundry)).statement
            data = pd.read_sql(query, db.engine, index_col="date")
            lm = linear_model.LinearRegression()
            x = data['index'].values.reshape(-1, 1)
            y = data['stock'].values.reshape(-1, 1)
            lm.fit(x, y)
            coefficients.append(CapmCoefficients(ticker=current_ticker, alpha=lm.intercept_.item(0), beta=lm.coef_.item(0)))
        
        db.session.bulk_save_objects(coefficients)
        db.session.commit()
        
    
    @staticmethod
    def calculate_training_errors():
        db.session.query(EquityErrors).delete()
        db.session.commit()
        container = pd.DataFrame()
        
        all_tickers = []
        
        for result in db.session.query(EquityHistorical.ticker).filter(EquityHistorical.date.between(DataWrangling.training_data_begin, DataWrangling.training_data_end)).distinct().all():
            all_tickers.append(result.ticker)
            
        for ticker in all_tickers:
            
            capm_params = db.session.query(CapmCoefficients).filter(CapmCoefficients.ticker == ticker).first()
            
            query = db.session.query(EquityReturns.date.label("date"), EquityReturns.percent_return.label("stock"), NasdaqGlobalEquityReturns.percent_return.label("index")).join(NasdaqGlobalEquityReturns, EquityReturns.date == NasdaqGlobalEquityReturns.date).filter((EquityReturns.ticker == ticker) & (EquityReturns.date <= DataWrangling.training_data_end) & (EquityReturns.date >= DataWrangling.training_data_begin)).statement
        
            data = pd.read_sql(query, db.engine, index_col="date")
            
            data['error'] = data['stock'] - data['index']*capm_params.beta - capm_params.alpha
            data['ticker'] = ticker
            data.drop(['stock', 'index'], inplace=True, axis=1)
            data.dropna(inplace=True)
            
            container = container.append(data)
            
            
        container.to_sql('equity_errors', db.engine, if_exists='append', index=True, chunksize=5000)
            
            
            
            
        
        
        
        
            
        
    
            
            
            
        

    
        
