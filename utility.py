import pandas as pd
import psycopg2
import os
from threading import get_ident
import csv

#ssh -p 33257 rayford@71.220.185.82

def copy_dataframe_to_database(dataframe, model, with_index=True):
    table_name = model.__tablename__
    csv_name = "{target_table}_{thread_id}_upload.csv".format(target_table = table_name, thread_id = get_ident())
    dataframe.to_csv(csv_name, index=with_index, chunksize=5000)
    connection = psycopg2.connect('dbname=marketdata user=bojangles password=apppas0! host=localhost port=5432')
    cursor = connection.cursor()
    copy_sql_string = """COPY {target_table} FROM STDIN WITH CSV HEADER DELIMITER AS ','""".format(target_table = table_name)
    file_to_write = open(csv_name)
    cursor.copy_expert(sql=copy_sql_string, file=file_to_write)
    connection.commit()
    cursor.close()
    os.remove(csv_name)

def convert_training_labels(read_path, write_path):
    vocab = ['-51', '-52', '-53', '-54', '-55', '-56', '-57',
             '-41', '-42', '-43', '-44', '-45', '-46', '-47',
             '-31', '-32', '-33', '-34', '-35', '-36', '-37',
             '-21', '-22', '-23', '-24', '-25', '-26', '-27',
             '-11', '-12', '-13', '-14', '-15', '-16', '-17',
             '11', '12', '13', '14', '15', '16', '17',
             '21', '22', '23', '24', '25', '26', '27',
             '31', '32', '33', '34', '35', '36', '37',
             '41', '42', '43', '44', '45', '46', '47',
             '51', '52', '53', '54', '55', '56', '57']
                                                                                                                       
    with open(read_path) as file:
        reader = csv.DictReader(file)
        with open(write_path, 'w') as new_file:
            writer = csv.DictWriter(new_file, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                #dont want zeros for labels
                new_value = vocab.index(row['label']) + 1
                row['label'] = new_value
                writer.writerow(row)
