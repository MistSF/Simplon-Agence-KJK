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

ad.createDatabase("Agence-KJK")

print("Welcome in agency data manager V1.0")

while run :
    entry = input()

    if entry == "quit" :
        run = False