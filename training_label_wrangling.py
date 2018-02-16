from queue import Queue
from models import OptionTrainingLabels, OptionHistorical
from app import db
from thread_workers import LongCallWorker
from time import sleep
from datetime import datetime



def calculate_long_call_training_labels():
    all_dates = []
    scrapped_dates = []
    for value in db.session.query(OptionHistorical.trade_date).distinct():
        all_dates.append(value.trade_date)
    print("[{timestamp}]: Finished all dates".format(timestamp=datetime.now()))
    for value in db.session.query(OptionTrainingLabels.trade_date).filter(OptionTrainingLabels.trade_type == "Long Call").distinct():
        scrapped_dates.append(value.trade_date)
    print("[{timestamp}]: Finished scrapped dates".format(timestamp=datetime.now()))
    dates_to_do = set(all_dates) - set(scrapped_dates)
    print("[{timestamp}]: Creating queue".format(timestamp=datetime.now()))
    queue = Queue()
    for i in dates_to_do:
        queue.put(i)
    print("[{timestamp}]: Spawning workers".format(timestamp=datetime.now()))
    for x in range(2):
        print("[{timestamp}]: Create Worker {num}".format(timestamp=datetime.now(), num = x+1))
        worker = LongCallWorker(queue)
        worker.daemon = True
        worker.start()
        print("[{timestamp}]: Start Worker {num}".format(timestamp=datetime.now(), num=x+1))
    queue.join()
    
    #for day in dates_to_do:
    #    print("[{timestamp}]: Working on {trade_day}".format(timestamp=datetime.now(), trade_day=day))
    #    query_string = """WITH max_price_data AS (SELECT ticker, experiation_date, strike, max(call_bid_price) as max_sales_price FROM option_historical WHERE trade_date >= to_date('{day_working_on}', 'YYYY-MM-DD') AND (ticker, experiation_date, strike) = ANY(SELECT ticker, experiation_date, strike FROM option_historical WHERE trade_date = to_date('{day_working_on}', 'YYYY-MM-DD')) GROUP BY ticker, experiation_date, strike)
    #                      SELECT option_historical.ticker, option_historical.experiation_date, option_historical.strike, option_historical.trade_date, option_historical.call_ask_price as buy_price, max_price_data.max_sales_price
    #                      FROM option_historical JOIN max_price_data
    #                      ON option_historical.ticker = max_price_data.ticker
    #                      AND option_historical.experiation_date = max_price_data.experiation_date
    #                      AND option_historical.strike = max_price_data.strike
    #                      AND option_historical.trade_date = to_date('{day_working_on}', 'YYYY-MM-DD')
    #                      """.format(day_working_on = day)
    #    
    #    data = pd.read_sql(query_string, db.engine)
    #    data['trade_type'] = "Long Call"
    #    data['label'] = (data['max_sales_price'] - data['buy_price']) / data['buy_price']
    #    data.drop(['buy_price', 'max_sales_price'], inplace=True, axis=1)
    #    data.dropna(inplace=True)
    #    copy_dataframe_to_database(data, OptionTrainingLabels, with_index=False)