# Hbase_Python_Programs

# Importing CSV FILE INTO HBASE TABLE USING HAPPYBASE 

# START THE THRIFT SERVER SERVER BY DOING
* hbase-daemon.sh start thrift
* There are several methods written for that particular tasks
* Firstly create a connection with hbase using happybase

# The Following Functions Has Been Added Inside Hbase.Py FIle:
* connect_to_hbase():This function is used for creating connection with hbase
* creating_table(): This function is used for creating hbase table
* put_csv_data_into_hbase():  This function is used for putting csv data into hbase table.
* display_table_data(): This function is used for displaying data from hbase table.

# Csv File is Added To the Input Folder:
* we can use other csv file for the operation by add it into input folder.

# Note:- In Case of Large Data to Load We have To Use Batch() To Put Our Data In Hbase Table.
* just make the changes according to requirement because Table.Put() method is slow.
