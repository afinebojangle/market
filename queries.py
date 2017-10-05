results = session.execute(select([a.c.close - over(func.lag(a.c.adj_close), order_by = a.c.date)]).where(and_(between(a.c.date, datetime.date(2017, 10, 2), datetime.date(2017, 10, 4)), a.c.ticker=="O")))

results2 = session.execute(select([a.c.adj_close, a.c.close]).where(and_(between(a.c.date, datetime.date(2017, 10, 2), datetime.date(2017, 10, 4)), a.c.ticker=="O")))