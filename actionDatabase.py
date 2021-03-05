import mysql.connector
import pandas as pd

TABLES = {}

TABLES["Geolocation"] = ("""
    CREATE TABLE IF NOT EXISTS Geolocation (
        zip_code_prefix VARCHAR(10) NOT NULL PRIMARY KEY,
        lat FLOAT NOT NULL,
        lng FLOAT NOT NULL,
        city VARCHAR(20),
        state VARCHAR(20)
    )"""
)

TABLES["Customers"] = ("""
    CREATE TABLE IF NOT EXISTS Customers (
        customer_id VARCHAR(40) NOT NULL PRIMARY KEY,
        customer_unique_id VARCHAR(30) NOT NULL, 
        customer_zip_code_prefix VARCHAR(10) NOT NULL,
        customer_city VARCHAR(20) NOT NULL,
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
        product_category_name VARCHAR(40),
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

def loadData(cursor, path, name) :
    data = pd.read_csv(path)
    name = data.columns
    for x in name :
        print(x)