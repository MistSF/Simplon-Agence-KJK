import mysql.connector
import config as cfg
import actionDatabase as ad
import dictionnary as dc

mydb = mysql.connector.connect(
    host=cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["passwd"],
    auth_plugin='mysql_native_password'
)

run = True

cursor = mydb.cursor(buffered=True)

cursor = mydb.cursor(buffered=True)
ad.createDatabase(cursor, "Agence_KJK")


print("Welcome in agency data manager V1.0")

while run :
    entry = input().lower()

    if entry in dc.REQUEST :
        ad.getRequest(cursor, dc.REQUEST[entry])
    if entry == "quit" :
        run = False
    elif entry == "help" :
        ad.help()
    elif entry == "get nb customers" :
        ad.getNB(cursor, "Customers")
    elif entry == "get nb products" :
        ad.getNB(cursor, "Products")
    elif entry == "get nb orders" :
        ad.getNB(cursor, "Orders")
    elif entry == "get nb sellers" :
        ad.getNB(cursor, "Sellers")
    elif entry == "show products" :
        ad.getShow(cursor, "Products")
    elif entry == "show avg payment" :
        ad.getAvgNB(cursor)
    elif entry == "get average basket" :
        ad.getAvgNB(cursor)
    elif entry == "new product" :
        ad.newProduct(cursor, mydb)