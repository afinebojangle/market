from sqlalchemy import func
import quandl
from datetime import timedelta, date, datetime
from models import EquityHistorical, EquityReturns, NasdaqGlobalEquityIndex, NasdaqGlobalEquityReturns, CapmCoefficients, EquityErrors, OptionHistorical, EquityVolatilities, OptionTrainingLabels
import pandas as pd
from sklearn import linear_model
from app import db
from utility import copy_dataframe_to_database


class DataWrangling():
   
    data_begin_date = date(2012, 12, 31)
    equity_data_begin_date = date(2010, 12, 31)
    training_metric_boundry = date(2015, 12, 31)
    training_data_begin = date(2016, 1, 1)
    training_data_end = date(2016, 12, 31)
    option_data_begin_date = date(2013, 1, 2)
    
    @staticmethod
    def calculate_equity_returns():
        
        db.session.query(EquityReturns).delete()
        db.session.commit()
        
        tickers = []
        completed_tickers = []
        tickers_to_do = []
        
        for result in db.session.query(EquityHistorical.ticker).distinct().all():
            tickers.append(result.ticker)
            
        for result in db.session.query(EquityReturns.ticker).distinct().all():
            completed_tickers.append(result.ticker)
            
        tickers_to_do = set(tickers) - set(completed_tickers)
            
        for current_ticker in tickers_to_do:
            date_boundry = [DataWrangling.equity_data_begin_date]
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
            copy_dataframe_to_database(data, EquityReturns)

            
    @staticmethod
    def calculate_equity_volatilities():
        tickers = []
        completed_tickers = []
        tickers_to_do = []
        
        db.session.query(EquityVolatilities).delete()
        db.session.commit()
        
        for result in db.session.query(EquityHistorical.ticker).distinct().all():
            tickers.append(result.ticker)
        
        for result in db.session.query(EquityVolatilities.ticker).distinct().all():
            completed_tickers.append(result.ticker)
            
        tickers_to_do = set(tickers) - set(completed_tickers)
        
        for current_ticker in tickers_to_do:
            query = db.session.query(EquityReturns).filter(EquityReturns.ticker == current_ticker).order_by(EquityReturns.date.asc()).statement
            data = pd.read_sql(query, db.engine)
            data['ma_10'] = data['nominal_return'].rolling(window=10, center=False).std()
            data['ma_50'] = data['nominal_return'].rolling(window=50, center=False).std()
            data['ma_100'] = data['nominal_return'].rolling(window=100, center=False).std()
            data['ma_250'] = data['nominal_return'].rolling(window=250, center=False).std()
            data.fillna(0, inplace=True)
            data.drop(['nominal_return', 'percent_return'], inplace=True, axis=1)
            column_order = [m.key for m in EquityVolatilities.__table__.columns]
            data = data[column_order]
            copy_dataframe_to_database(data, EquityVolatilities, with_index=False)
            
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
        scrapped_dates = [date.today()]

        for value in db.session.query(EquityHistorical.date).distinct():
            scrapped_dates.append(value.date)
        
        date_range = []
        
        for n in range((date.today() - DataWrangling.equity_data_begin_date).days):
                date_range.append(DataWrangling.equity_data_begin_date + timedelta(n))
                
        dates_to_do = set(date_range) - set(scrapped_dates)
        
        for day in dates_to_do:
            data = quandl.get_table('WIKI/PRICES', date=day.strftime('%Y%m%d'))
            data.rename(columns={'ex-dividend': 'ex_dividend'}, inplace=True)
            data.dropna(inplace=True)
            data['volume'] = data['volume'].astype(int)
            data['adj_volume'] = data['adj_volume'].astype(int)
            data.to_csv('equity_historical_upload.csv', index=False, chunksize=5000)
            connection = psycopg2.connect('dbname=marketdata user=bojangles password=apppas0! host=marketdata.chew6qxftqgr.us-east-1.rds.amazonaws.com port=5432')
            cursor = connection.cursor()
            copy_sql_string = """COPY {target_table} FROM STDIN WITH CSV HEADER DELIMITER AS ','""".format(target_table = EquityHistorical.__tablename__)
            file_to_write = open("equity_historical_upload.csv")
            cursor.copy_expert(sql=copy_sql_string, file=file_to_write)
            connection.commit()
            cursor.close()
            os.remove("equity_historical_upload.csv")
            
            
                
                
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
            
            if capm_params == None:
                pass
            else: 
                query = db.session.query(EquityReturns.date.label("date"), EquityReturns.percent_return.label("stock"), NasdaqGlobalEquityReturns.percent_return.label("index")).join(NasdaqGlobalEquityReturns, EquityReturns.date == NasdaqGlobalEquityReturns.date).filter((EquityReturns.ticker == ticker) & (EquityReturns.date <= DataWrangling.training_data_end) & (EquityReturns.date >= DataWrangling.training_data_begin)).statement
            
                data = pd.read_sql(query, db.engine, index_col="date")
                
                data['error'] = data['stock'] - data['index']*capm_params.beta - capm_params.alpha
                data['ticker'] = ticker
                data.drop(['stock', 'index'], inplace=True, axis=1)
                data.dropna(inplace=True)
                
                container = container.append(data)
            
            
        container.to_sql('equity_errors', db.engine, if_exists='append', index=True, chunksize=25000)
        
    @staticmethod
    def refresh_option_data():
        
        scrapped_dates = [date.today()]

        for value in db.session.query(OptionHistorical.trade_date).distinct():
            scrapped_dates.append(value.trade_date)
        
        date_range = []
        
        option_date_range = pd.read_csv('https://www.quandl.com/api/v3/datatables/OR/OSMVDATES.csv?&api_key=iasNNCL-zjwhdxME_Jvx')
        
        for n in option_date_range['trade_date'].unique():
                date_range.append(datetime.strptime(n, '%Y-%m-%d').date())
                
        dates_to_do = set(date_range) - set(scrapped_dates)
        
        
        for day in dates_to_do:
            print("[{timestamp}]: Working on {trade_day}".format(timestamp=datetime.now(), trade_day=day))
            api_query_string = 'https://www.quandl.com/api/v3/databases/OSMV/download?download_type={date}&api_key={api_key}'.format(date=day.strftime('%Y%m%d'), api_key='iasNNCL-zjwhdxME_Jvx')
            data = pd.read_csv(api_query_string, compression='zip')
            column_names = {
                'expirDate': 'experiation_date',
                'stkPx': 'stock_price',
                'yte': 'years_to_expiration',
                'cVolu': 'call_volume',
                'cOi': 'call_open_interest',
                'pVolu': 'put_volume',
                'pOi': 'put_open_interest',
                'cBidPx': 'call_bid_price',
                'cValue': 'call_theoretical_value',
                'cAskPx': 'call_ask_price',
                'pBidPx': 'put_bid_price',
                'pValue': 'put_theoretical_value',
                'pAskPx': 'put_ask_price',
                'cBidIv': 'call_bid_implied_volitility',
                'cMidIv': 'call_mid_market_implied_volitility',
                'cAskIv': 'call_ask_implied_volitility',
                'smoothSmvVol': 'smoothed_strike_implied_volitility',
                'pBidIv': 'put_bid_implied_volitility',
                'pMidIv': 'put_mid_market_implied_volitility',
                'pAskIv': 'put_ask_implied_volitility',
                'iRate': 'risk_free_interest_rate',
                'divRate': 'dividend_rate',
                'residualRateData': 'residual_rate_data',
                'driftlessTheta': 'driftless_theta',
                'extVol': 'extended_volitility',
                'extCTheo': 'extended_call_theoretical_price',
                'extPTheo': 'extended_put_theoretical_price',
            }
            data.rename(columns=column_names, inplace=True)
            data.drop(['spot_px'], inplace=True, axis=1)
            data.drop_duplicates(subset=['trade_date', 'ticker', 'strike', 'experiation_date'], keep=False, inplace=True)
            data = data[(data.delta != 0) & (data.gamma != 0) & (data.theta != 0) & (data.vega != 0) & (data.rho != 0) & (data.phi != 0)]
            data.fillna(0, inplace=True)
            column_order = [m.key for m in OptionHistorical.__table__.columns]
            data = data[column_order]
            data.to_csv('option_historical_upload.csv', index=False, chunksize=5000)
            connection = psycopg2.connect('dbname=marketdata user=bojangles password=apppas0! host=marketdata.chew6qxftqgr.us-east-1.rds.amazonaws.com port=5432')
            cursor = connection.cursor()
            copy_sql_string = """COPY {target_table} FROM STDIN WITH CSV HEADER DELIMITER AS ','""".format(target_table = OptionHistorical.__tablename__)
            file_to_write = open("option_historical_upload.csv")
            cursor.copy_expert(sql=copy_sql_string, file=file_to_write)
            connection.commit()
            cursor.close()
            os.remove("option_historical_upload.csv")   
            
    @staticmethod
    def calculate_long_call_training_labels():
        all_dates = []
        scrapped_dates = []
        for value in db.session.query(OptionHistorical.trade_date).distinct():
            all_dates.append(value.trade_date)
        completed_dates = []
        for value in db.session.query(OptionTrainingLabels.trade_date).filter(OptionTrainingLabels.trade_type == "Long Call").distinct():
            scrapped_dates.append(value.trade_date)
        dates_to_do = set(all_dates) - set(completed_dates)
        for day in dates_to_do:
            print("[{timestamp}]: Working on {trade_day}".format(timestamp=datetime.now(), trade_day=day))
            #base_data_query = db.session.query(OptionHistorical.ticker, OptionHistorical.experiation_date, OptionHistorical.strike, OptionHistorical.trade_date, OptionHistorical.call_ask_price).filter(OptionHistorical.trade_date == day).statement
            #max_price_query = db.session.query(OptionHistorical.ticker, OptionHistorical.experiation_date, OptionHistorical.strike, OptionHistorical.trade_date, func.max(OptionHistorical.call_bid_price).label("max_sale_price")).group_by(OptionHistorical.ticker, OptionHistorical.experiation_date, OptionHistorical.strike, OptionHistorical.trade_date).filter(OptionHistorical.trade_date >= day).statement
            #base_data_df = pd.read_sql(base_data_query, db.engine)
            #max_price_df = pd.read_sql(max_price_query, db.engine)
            
            query_string = """WITH max_price_data AS (SELECT ticker, experiation_date, strike, max(call_bid_price) as max_sales_price FROM option_historical WHERE trade_date >= to_date('{day_working_on}', 'YYYY-MM-DD') GROUP BY ticker, experiation_date, strike)
                              SELECT option_historical.ticker, option_historical.experiation_date, option_historical.strike, option_historical.trade_date, option_historical.call_ask_price as buy_price, max_price_data.max_sales_price
                              FROM option_historical JOIN max_price_data
                                ON option_historical.ticker = max_price_data.ticker
                                AND option_historical.experiation_date = max_price_data.experiation_date
                                AND option_historical.strike = max_price_data.strike
                                 """.format(day_working_on = day) 
            
            data = pd.read_sql(query_string, db.engine)
            data['trade_type'] = "Long Call"
            data['label'] = (data['max_sales_price'] - data['buy_price']) / data['buy_price']
            data.drop(['buy_price', 'max_sales_price'], inplace=True, axis=1)
            data.dropna(inplace=True)
            copy_dataframe_to_database(data, OptionTrainingLabels, with_index=False)
