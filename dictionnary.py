import actionDatabase as ad

TABLES = {}
REQUEST = {}

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

REQUEST["average orders by day"] = """
    SELECT AVG(byday) AS AVERAGE
    FROM (SELECT order_purchase_timestamp, COUNT(*) AS byday
    FROM `Agence_KJK`.`Orders` GROUP BY order_purchase_timestamp) AS A
    """
REQUEST["delivery time by month"] = """
    SELECT AVG(DATEDIFF(order_delivered_customer_date, order_delivered_carrier_date)) AS average_delivery_time, EXTRACT(YEAR_MONTH FROM order_purchase_timestamp) AS purchase
    FROM `Agence_KJK`.`Orders`
    GROUP BY purchase
    ORDER BY purchase
"""
REQUEST["get nb orders by cities"] = """
    SELECT Sellers.seller_city, COUNT(*) 
    FROM Order_items
    INNER JOIN Sellers ON Order_items.seller_id = Sellers.seller_id
    GROUP BY seller_city
    ORDER BY seller_city
"""
REQUEST["get nb products by category"] = """
    SELECT product_category_name, COUNT(*) FROM Products GROUP BY product_category_name
"""
REQUEST["average delivery time"] = """
    SELECT AVG(DATEDIFF(order_delivered_customer_date, order_delivered_carrier_date)) FROM Orders ; 
"""
REQUEST["sellers by state"] = """
    SELECT seller_state, COUNT(*) 
    FROM Sellers 
    GROUP BY seller_state;
"""
REQUEST["get orders by month"] = """
    SELECT  EXTRACT(YEAR_MONTH FROM order_purchase_timestamp) AS YM, COUNT(*)
    FROM  `Agence_KJK`.`Orders`
    GROUP BY YM
    ORDER BY YM
"""
REQUEST["get orders by state"] = """
    SELECT order_status, COUNT(*) 
    FROM `Agence_KJK`.`Orders` 
    GROUP BY order_status;
"""

REQUEST["average orders score"] = """
    SELECT AVG(review_score) FROM Order_reviews;
"""

REQUEST["get nb selled by category"] = """
    SELECT Products.Product_category_name, COUNT(*) 
    FROM Order_items
    INNER JOIN Products ON Order_items.product_id = Products.product_id
    GROUP BY Product_category_name
    ORDER BY Product_category_name
"""

REQUEST["order min price"] ="""
    SELECT MIN(payment_value) FROM Order_payments ;
"""

REQUEST["order max price"] ="""
    SELECT MAX(payment_value) FROM Order_payments ;
"""