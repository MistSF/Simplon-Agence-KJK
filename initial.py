import mysql.connector
import config as cfg
import actionDatabase as ad

mydb = mysql.connector.connect(
    host=cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["passwd"],
    auth_plugin='mysql_native_password'
)

cursor = mydb.cursor(buffered=True)

ad.createTable(cursor)
ad.createDatabase(cursor, "Agence_KJK")

ad.loadData(cursor, "./Data/olist_geolocation_dataset.csv", "Geolocation", mydb)
ad.loadData(cursor, "./Data/olist_customers_dataset.csv", "Customers", mydb)
ad.loadData(cursor, "./Data/olist_orders_dataset.csv", "Orders", mydb)
ad.loadData(cursor, "./Data/olist_order_reviews_dataset.csv", "Order_reviews", mydb)
ad.loadData(cursor, "./Data/olist_order_payments_dataset.csv", "Order_payments", mydb)
ad.loadData(cursor, "./Data/olist_sellers_dataset.csv", "Sellers", mydb)
ad.loadData(cursor, "./Data/olist_products_dataset.csv", "Products", mydb)
ad.loadData(cursor, "./Data/olist_order_items_dataset.csv", "Order_items", mydb)
