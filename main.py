import mysql.connector
import config as cfg
import actionDatabase as ad

mydb = mysql.connector.connect(
    host=cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["passwd"],
    auth_plugin='mysql_native_password'
)

run = True

cursor = mydb.cursor(buffered=True)

ad.createDatabase(cursor, "Agence_KJK")
ad.createTable(cursor)

print("Welcome in agency data manager V1.0")

# ad.loadData(cursor, "./Data/olist_customers_dataset.csv", "Customers")

while run :
    entry = input()

    if entry == "quit" :
        run = False