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

print("Welcome in agency data manager V1.0")

while run :
    entry = input()

    if entry == "quit" :
        run = False
    elif entry == "get nb customers" :
        ad.getNB(cursor, "Customers")
    elif entry == "get nb products" :
        ad.getNB(cursor, "Products")
    elif entry == "get nb orders" :
        ad.getNB(cursor, "Orders")
    elif entry == "get nb sellers" :
        ad.getNB(cursor, "Sellers")
    elif entry == "get orders by state" :
        ad.getOrdersBy(cursor, "state")
    elif entry == "get orders by month" :
        ad.getOrdersBy(cursor, "month")
    elif entry == "show products" :
        ad.getShow(cursor, "Products")