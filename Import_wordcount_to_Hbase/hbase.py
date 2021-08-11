"""
@Author: Rikesh Chhetri
@Date: 2021-08-11
@Last Modified by: Rikesh Chhetri
@Last Modified time: 2021-08-11 10:03:30
@Title : Program Aim is to import csv file into hbase table using happybase.
"""

import happybase as hb
from loghandler import logger
import csv
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
        connection.create_table('wordcount',{'cf1': dict(max_versions=1),'cf2': dict(max_versions=1)})
        logger.info("table created successfully")
       
    except Exception as e: 
        logger.error(e)
        connection.close()




creating_table()



