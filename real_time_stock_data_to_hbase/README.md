# Hbase_Programs

# Load Real Time Stock Data Into Hbase Using Happybase

* Version used:
* HappyBase 1.2.0
* Release date: 2019-05-14

#STEPS FOR LOADING REalTime Stock Data INTO HBASE USING HAPPY BASE:
# START THE THRIFT SERVER SERVER BY DOING 

*hbase-daemon.sh start thrift
Firstly ,
# create a connection with hbase using happybase

* NOTE:- We have to get our real time stock data from alphavantage
* And put that into list and using for loop we will be adding our data
* Into out hbase table.

* Use .env file to store api keys.

# Firstly import necessary library:

import happybase as hb
import requests
from loghandler import logger
import csv
import os
from dotenv import load_dotenv
load_dotenv('.env')
 

# Now write a python code for connection:

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

# CODE FOR CREATING TABLE :
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
 
 

# CODE FOR GETTING REAL TIME STOCK DATA AND PUTTING IT  INTO HBASE:

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
 
       
 

# CODE FOR DISPLAYING RECORDS FROM HBASE TABLE:

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
 


# FOR RUNNING THE ABOVE FUNCTION:
 
* creating_table()
* put_csv_data_into_hbase()
* display_table_data()





