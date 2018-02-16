from datetime import datetime
from threading import Thread, local
from models import OptionTrainingLabels
import pandas as pd
from app import db
from utility import copy_dataframe_to_database

class LongCallWorker(Thread):
    def __init__(self, queue):
       Thread.__init__(self)
       self.queue = queue
    
    def run(self):
        while True:
            day = self.queue.get()
            if day is None:
                break
            print("[{timestamp}]: Starting on {trade_day}".format(timestamp=datetime.now(), trade_day=day))
            query_string = """WITH max_price_data AS (SELECT ticker, experiation_date, strike, max(call_bid_price) as max_sales_price FROM option_historical WHERE trade_date >= to_date('{day_working_on}', 'YYYY-MM-DD') AND (ticker, experiation_date, strike) = ANY(SELECT ticker, experiation_date, strike FROM option_historical WHERE trade_date = to_date('{day_working_on}', 'YYYY-MM-DD')) GROUP BY ticker, experiation_date, strike)
                              SELECT option_historical.ticker, option_historical.experiation_date, option_historical.strike, option_historical.trade_date, option_historical.call_ask_price as buy_price, max_price_data.max_sales_price
                              FROM option_historical JOIN max_price_data
                              ON option_historical.ticker = max_price_data.ticker
                              AND option_historical.experiation_date = max_price_data.experiation_date
                              AND option_historical.strike = max_price_data.strike
                              AND option_historical.trade_date = to_date('{day_working_on}', 'YYYY-MM-DD')
                              """.format(day_working_on = day)
                              
            thread_variable = local()
            thread_variable.data = pd.read_sql(query_string, db.engine)
            thread_variable.data['trade_type'] = "Long Call"
            thread_variable.data['label'] = (thread_variable.data['max_sales_price'] - thread_variable.data['buy_price']) / thread_variable.data['buy_price']
            thread_variable.data.drop(['buy_price', 'max_sales_price'], inplace=True, axis=1)
            thread_variable.data.dropna(inplace=True)
            copy_dataframe_to_database(thread_variable.data, OptionTrainingLabels, with_index=False)
            
            
            #data = pd.read_sql(query_string, db.engine)
            #data['trade_type'] = "Long Call"
            #data['label'] = (data['max_sales_price'] - data['buy_price']) / data['buy_price']
            #data.drop(['buy_price', 'max_sales_price'], inplace=True, axis=1)
            #data.dropna(inplace=True)
            #copy_dataframe_to_database(data, OptionTrainingLabels, with_index=False)

            
            print("[{timestamp}]: Finished {trade_day}".format(timestamp=datetime.now(), trade_day=day))
            
            self.queue.task_done()