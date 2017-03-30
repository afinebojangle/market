from sqlalchemy import create_engine

def connect():

    engine = create_engine('postgresql://rayford:rhtpas0!@localhost:5432/market_data', echo=True)

    connection = engine.connect()
