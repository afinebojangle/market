import quandl

quandl.ApiConfig.api_key = 'iasNNCL-zjwhdxME_Jvx'

data = quandl.get_table('WIKI/PRICES', date='20170329')

data.rename(columns={'ex-dividend': 'ex_dividend'}, inplace=True)

data.to_sql('equity_historical', engine, if_exists='append', index=False, chunksize=5000)
