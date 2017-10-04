import quandl

quandl.ApiConfig.api_key = 'iasNNCL-zjwhdxME_Jvx'

data = quandl.get_table('WIKI/PRICES', date='20170329')

data.rename(columns={'ex-dividend': 'ex_dividend'}, inplace=True)

data.to_sql('equity_historical', engine, if_exists='append', index=False, chunksize=5000)



data.index.names=['date']

data = quandl.get('NASDAQOMX/NQGI')

data.rename(columns={'Index Value': 'value', 'High': 'high', 'Low': 'low', 'Total Market Value': 'market_value', 'Dividend Market Value': 'dividend_market_value'}, inplace=True)

data.to_sql('nasdaq_global_equity_index', engine, if_exists='append', index=True, chunksize=5000)
