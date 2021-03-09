from os import name
import numpy as np
import mysql.connector
import pandas as pd
import string
import random
import dictionnary as dc

def get_random_string(length):
    letters = string.ascii_letters + "0123456789"
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def showCursor(cursor) :
    return pd.Series(cursor.fetchall())

def saveError(name, request, value) :
    f = open("./Error/{}.txt".format(name), "a")
    f.write("{} : {}\n{}\n\n".format(name, request, value))
    f.close()

def createDatabase(cursor, database) :
    request = "CREATE DATABASE {}".format(database)
    try :
        cursor.execute(request)
    except mysql.connector.Error as err :
        print("creation failed : {}".format(err))
    
    request = "USE {}".format(database)
    try :
        cursor.execute(request)
    except mysql.connector.Error as err :
        print("connection failed :  {}".format(err) )

def createTable(cursor) :
    for x in dc.TABLES :
        cursor.execute(dc.TABLES[x])

def loadData(cursor, path, name, mydb) :
    print(name)
    data = pd.read_csv(path)
    data.drop_duplicates(subset=[data.columns[0]], keep="first", inplace=True)
    for x in data.iloc(0) :
        request = "INSERT INTO {} VALUES (".format(name)
        for y in x :
            if type(y) == str :
                request = request + "\"" + str(y) + "\", "
            elif str(y) == 'nan' :
                request = request + "NULL , "
            else :
                request =request + str(y) + ", "
        request = request[:-2] + ")"
        try :
            cursor.execute(request)
        except mysql.connector.Error as err :
            saveError(name, request, err)
            print(err)

    mydb.commit()
    print()

def getNB(cursor, table) :
    try :
        cursor.execute("SELECT COUNT(0) FROM {}".format(table))
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)

def getAvgNB(cursor) :
    try :
        cursor.execute("SELECT AVG(payment_value) FROM Order_payments")
        res = cursor.fetchall()
        print(round((res[0][0]),2))
        res = showCursor(cursor)
    except mysql.connector.Error as err :
            print(err)    

def newProduct(cursor, mydb) : 
    try :
        cursor.execute("SELECT product_id FROM Products")
        res = showCursor(cursor)
        add = False
        while add == False:
            add = True
            newID = get_random_string(40)
            print(newID)
            for x in res :
                if x[0] == newID :
                    add = False
        print("category name :")
        newCategory = input()
        cursor.execute("""
            INSERT INTO Products VALUES (
            '{}','{}',{},
            {},{},{},
            {},{},{}
            )""".format(
                newID, newCategory, np.random.choice(1000),
                np.random.choice(1000), np.random.choice(1000), np.random.choice(1000),
                np.random.choice(1000), np.random.choice(1000), np.random.choice(1000)
            ))
        mydb.commit()
    except mysql.connector.Error as err :
        print(err)

def getRequest(cursor, sql) :
    try :
        cursor.execute(sql)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)
