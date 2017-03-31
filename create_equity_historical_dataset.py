from sqlalchemy import create_engine
import quandl
from sqlalchemy.orm import sessionmaker
from datetime import timedelta, date


engine = create_engine('postgresql://rayford:rhtpas0!@localhost:5432/market_data', echo=True)

Session = sessionmaker(bind=engine)

session = Session()

quandl.ApiConfig.api_key = 'iasNNCL-zjwhdxME_Jvx'

scraped_dates = session.query(EquityHistorical.date).distinct().all()

date_range = []

for n in range((date.today() - date(2017, 3, 29)).days):
        date_range.append(date(2017, 3, 29) + timedelta(n))

call_count = 0

while call_count < 50000:

    for date in date_range:
        if date.weekday() > 5:
            pass

        if date in scraped_dates:
            pass
