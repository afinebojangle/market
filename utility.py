import pandas as pd
import psycopg2
import os


def copy_dataframe_to_database(dataframe, model, with_index=True):
    table_name = model.__tablename__
    csv_name = "{target_table}_upload.csv".format(target_table = table_name)
    dataframe.to_csv(csv_name, index=with_index, chunksize=5000)
    connection = psycopg2.connect('dbname=marketdata user=bojangles password=apppas0! host=marketdata.chew6qxftqgr.us-east-1.rds.amazonaws.com port=5432')
    cursor = connection.cursor()
    copy_sql_string = """COPY {target_table} FROM STDIN WITH CSV HEADER DELIMITER AS ','""".format(target_table = table_name)
    file_to_write = open(csv_name)
    cursor.copy_expert(sql=copy_sql_string, file=file_to_write)
    connection.commit()
    cursor.close()
    os.remove(csv_name)