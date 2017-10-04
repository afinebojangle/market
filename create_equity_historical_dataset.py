from sqlalchemy import create_engine
import quandl
from sqlalchemy.orm import sessionmaker
from datetime import timedelta, date


engine = create_engine('postgresql://bojangles:apppas0!@marketdata.chew6qxftqgr.us-east-1.rds.amazonaws.com:5432/marketdata', echo=True)

Session = sessionmaker(bind=engine)

session = Session()

quandl.ApiConfig.api_key = 'iasNNCL-zjwhdxME_Jvx'

scrapped_dates = []

for value in session.query(EquityHistorical.date).distinct():
    scrapped_dates.append(value.date)

date_range = []

for n in range((date.today() - date(2015, 3, 29)).days):
        date_range.append(date(2015, 3, 29) + timedelta(n))

call_count = 0

while call_count < 2:
    for day in date_range:
        print(day)
        if day.weekday() > 4:
            print(day.weekday() > 4)
        elif day in scrapped_dates:
            print(day in scrapped_dates)
        else:
            data = quandl.get_table('WIKI/PRICES', date=day.strftime('%Y%m%d'))
            data.rename(columns={'ex-dividend': 'ex_dividend'}, inplace=True)
            data.to_sql('equity_historical', engine, if_exists='append', index=False, chunksize=5000)
            scrapped_dates.append(day)
            call_count += 1
        
