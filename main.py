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
    elif entry == "show avg payment" :
        ad.getAvgNB(cursor)
    elif entry == "get average basket" :
        ad.getAvgNB(cursor)
    elif entry == "new product" :
        ad.newProduct(cursor, mydb)