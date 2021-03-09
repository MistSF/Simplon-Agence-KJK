from os import name
import numpy as np
import mysql.connector
import pandas as pd
import string
import random

TABLES = {}

TABLES["Geolocation"] = ("""
    CREATE TABLE IF NOT EXISTS Geolocation (
        zip_code_prefix VARCHAR(20) NOT NULL PRIMARY KEY,
        lat FLOAT NOT NULL,
        lng FLOAT NOT NULL,
        city VARCHAR(40),
        state VARCHAR(20)
    )"""
)

TABLES["Customers"] = ("""
    CREATE TABLE IF NOT EXISTS Customers (
        customer_id VARCHAR(40) NOT NULL PRIMARY KEY,
        customer_unique_id VARCHAR(40) NOT NULL, 
        customer_zip_code_prefix VARCHAR(20) NOT NULL,
        customer_city VARCHAR(40) NOT NULL,
        customer_state VARCHAR(20) NOT NULL,
        FOREIGN KEY (customer_zip_code_prefix) REFERENCES Geolocation(zip_code_prefix)
    )"""
)

TABLES["Orders"] = ("""
    CREATE TABLE IF NOT EXISTS Orders (
        order_id VARCHAR(40) NOT NULL PRIMARY KEY,
        customer_id VARCHAR(40) NOT NULL,
        order_status VARCHAR(20) NOT NULL,
        order_purchase_timestamp DATETIME NOT NULL,
        order_approved_at DATETIME NOT NULL,
        order_delivered_carrier_date DATETIME NOT NULL,
        order_delivered_customer_date DATETIME NOT NULL,
        order_estimated_delivery_date DATETIME NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
    )"""
)

TABLES["Order_reviews"] = ("""
    CREATE TABLE IF NOT EXISTS Order_reviews (
        review_id VARCHAR(40) NOT NULL PRIMARY KEY,
        order_id VARCHAR(40) NOT NULL,
        review_score TINYINT,
        review_comment_title VARCHAR(40),
        review_comment_message VARCHAR(40),
        review_creation_date DATETIME,  
        review_answer_timestamp DATETIME,
        FOREIGN KEY (order_id) REFERENCES Orders(order_id)
    )"""
)

TABLES["Order_payments"] = ("""
    CREATE TABLE IF NOT EXISTS Order_payments (
        order_id VARCHAR(40) NOT NULL,
        payment_sequential BOOLEAN,
        payment_type VARCHAR(40) NOT NULL,
        payment_installments TINYINT NOT NULL,
        payment_value FLOAT NOT NULL,
        FOREIGN KEY (order_id) REFERENCES Orders(order_id)
    )"""
)

TABLES["Sellers"] = ("""
    CREATE TABLE IF NOT EXISTS Sellers (
        seller_id VARCHAR(40) NOT NULL PRIMARY KEY,
        seller_zip_code_prefix VARCHAR(10) NOT NULL,
        seller_city VARCHAR(40),
        seller_state VARCHAR(40),
        FOREIGN KEY (seller_zip_code_prefix) REFERENCES Geolocation(zip_code_prefix)
    )"""
)

TABLES["Products"] = (""" 
    CREATE TABLE IF NOT EXISTS Products (
        product_id VARCHAR(40) NOT NULL PRIMARY KEY,
        product_category_name VARCHAR(80),
        product_name_lenght INT,
        product_description_lenght INT,
        product_photos_qty INT,
        product_weight_g INT,
        product_length_cm INT,
        product_height_cm INT,
        product_width_cm INT
    )"""
)

TABLES["Order_items"] = ("""
    CREATE TABLE IF NOT EXISTS Order_items (
        order_id VARCHAR(40),
        order_item_id VARCHAR(40),
        product_id VARCHAR(40),
        seller_id VARCHAR(40),
        shipping_limit_date DATETIME,
        price FLOAT ,
        freight_value FLOAT,
        FOREIGN KEY (order_id) REFERENCES Orders(order_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id),
        FOREIGN KEY (seller_id) REFERENCES Sellers(seller_id)
    )"""
)

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
    for x in TABLES :
        cursor.execute(TABLES[x])

def getNB(cursor, table) :
    try :
        cursor.execute("SELECT COUNT(0) FROM {}".format(table))
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)

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

def sellers_by_state(cursor):
    """function to select sellers by region """
    request = """
        SELECT seller_state, COUNT(*) 
        FROM Sellers 
        GROUP BY seller_state;"""
    try :
        cursor.execute(request)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err : 
        saveError(name, request, err)
        print(err)

def average_orders_score(cursor):
    """function that calculates the average score of all sellers """
    request = "SELECT AVG(review_score) FROM Order_reviews;"
    try :
        cursor.execute(request)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        saveError(name, request, err)
        print(err)

def orders_by_day(cursor,day):
    """function that displays the number of orders made on a specific day, the day attribute requests a date in the following form (yyyy / mm / jj)"""
    
    request = "SELECT COUNT(*) FROM Orders WHERE order_approved_at = %s ;"(day,)
    try : 
        cursor.execute(request)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)

def getOrdersBy(cursor, value) :
    try :
        if value == "state" :
            cursor.execute("""
                SELECT order_status, COUNT(*) 
                FROM `Agence_KJK`.`Orders` 
                GROUP BY order_status;""")
        elif value == "month" :
            cursor.execute("""
                SELECT  EXTRACT(YEAR_MONTH FROM order_purchase_timestamp) AS YM, COUNT(*)
                FROM  `Agence_KJK`.`Orders`
                GROUP BY YM
                ORDER BY YM
            """)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)


def order_price(cursor, value):
    """function which gives the smallest or the biggest value of the commands, in fonction of argument value"""
    request = "SELECT {}(payment_value) FROM Order_payments ;".format(value)
    try : 
        cursor.execute(request)
        res = showCursor(cursor)
        print(res) 
    except mysql.connector.Error as err :
        saveError(name, request, err)
        print(err)

def average_delivery_time(cursor):
    """function that calculates the average delivery time """
    request = "SELECT AVG(DATEDIFF(order_delivered_customer_date, order_delivered_carrier_date)) FROM Orders ; " 
    try : 
        cursor.execute(request)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        saveError(name, request, err)
        print(err)

def getNbProductsByCategory(cursor) :
    try :
        cursor.execute("SELECT product_category_name, COUNT(*) FROM Products GROUP BY product_category_name")
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)

def getAvgNB(cursor) :
    try :
        cursor.execute("SELECT AVG(payment_value) FROM Order_payments")
        res = cursor.fetchall()
        print(round((res[0][0]),2))
    except mysql.connector.Error as err :
            print(err)    

def getNbProductsSelledByCategory(cursor) :
    try :
        cursor.execute("""
            SELECT Products.Product_category_name, COUNT(*) 
            FROM Order_items
            INNER JOIN Products ON Order_items.product_id = Products.product_id
            GROUP BY Product_category_name
            ORDER BY Product_category_name
        """)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)


def getNbOrdersByCities(cursor) :
    try :
        cursor.execute("""
            SELECT Sellers.seller_city, COUNT(*) 
            FROM Order_items
            INNER JOIN Sellers ON Order_items.seller_id = Sellers.seller_id
            GROUP BY seller_city
            ORDER BY seller_city
        """)
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)

def avgDeliveryTimeByMonth(cursor) :
    try :
        cursor.execute("""
        SELECT AVG(DATEDIFF(order_delivered_customer_date, order_delivered_carrier_date)) AS average_delivery_time, EXTRACT(YEAR_MONTH FROM order_purchase_timestamp) AS purchase
        FROM `Agence_KJK`.`Orders`
        GROUP BY purchase
        ORDER BY purchase""")
        res = showCursor(cursor)
        print(res)
    except mysql.connector.Error as err :
        print(err)

def averageOrdersByDay(cursor) :
    try :
        cursor.execute("""
        SELECT AVG(byday) AS AVERAGE
        FROM (SELECT order_purchase_timestamp, COUNT(*) AS byday
        FROM `Agence_KJK`.`Orders` GROUP BY order_purchase_timestamp) AS A"""
        )
        res = showCursor(cursor)
        print(res)
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