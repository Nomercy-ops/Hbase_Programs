"""
@Author: Rikesh Chhetri
@Date: 2021-08-12
@Last Modified by: Rikesh Chhetri
@Last Modified time: 2021-08-12 10:03:30
@Title : Program Aim is to import real time stock data into hbase table using happybase.
"""

import happybase as hb
import requests
from loghandler import logger
import csv
import os
from dotenv import load_dotenv
load_dotenv('.env')
# Start thrift server first: hbase-daemon.sh start thrift


def connect_to_hbase():
    """
    Description:
        This function is used for creating connection with hbase
     Return:
        It return a conn 

    """
    try:
        conn = hb.Connection()
        conn.open()
        return conn
    except Exception as e:
        logger.error(e)


def creating_table():
    """
    Description:
        This function is used for creating hbase table

    """
    try:
        connection = connect_to_hbase()
        connection.create_table('reliance_stock_data', {'cf1': dict(max_versions=1), 'cf2': dict(max_versions=1), 'cf3': dict(
            max_versions=1), 'cf4': dict(max_versions=1), 'cf5': dict(max_versions=1)})
        logger.info("table created successfully")

    except Exception as e:
        logger.error(e)
        connection.close()
        
def put_csv_data_into_hbase():
    """
    Description:
        This function is used for putting csv data into hbase table

    """
    try:
        connection = connect_to_hbase()
        table = connection.table('reliance_stock_data')
        demo = os.getenv("API_KEY")
        CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&datatype=csv&symbol=RELIANCE.BSE&outputsize=compact&apikey={}'.format(
            demo)

        with requests.Session() as s:
            download = s.get(CSV_URL)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            next(cr)
            my_list = list(cr)
            for row in my_list:
                table.put(row[0],
                        {'cf1:Open': row[1],
                         'cf2:High': row[2],
                         'cf3:Low': row[3],
                         'cf4:Close': row[4],
                         'cf5:Volume': row[5]})
    except Exception as e:
        logger.error(e)
        connection.close()
        
def display_table_data():
    """
    Description:
        This function is used for displaying data from hbase table.

    """
    try:
        connection = connect_to_hbase()
        table = connection.table('reliance_stock_data')
        for key, data in table.scan():
            id = key.decode('utf-8')
            for value1, value2 in data.items():
                cf1 = value1.decode('utf-8')
                cf2 = value2.decode('utf-8')
                print(id,cf1,cf2)

    except Exception as e:
        logger.error(e)

creating_table()
put_csv_data_into_hbase()
display_table_data()



