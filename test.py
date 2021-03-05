import mysql.connector


mydb = mysql.connector.connect(
  host="localhost",
  user="KevinB", 
  password="ludo1703",
  database = "test"
)

print(mydb)
mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE customers(id_customers INT NOT NULL );")
mycursor.execute("LOAD DATA INFILE 'olist_customers_dataset.csv' INTO TABLE customers;")

