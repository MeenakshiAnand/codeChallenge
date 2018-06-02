from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'shfly'

TABLES = {}
TABLES['customer'] = (
    "CREATE TABLE `customer` ("
    "  `customer_id` varchar(50) NOT NULL,"
    "  `event_time` TIMESTAMP(6) NOT NULL,"
    "  `last_name` varchar(16) ,"
    "  `adr_city` varchar(16) ,"
    "  `adr_state` varchar(16) ,"
    "  PRIMARY KEY (`customer_id`)"
    ") ENGINE=InnoDB")


TABLES['site_visit'] = (
    "CREATE TABLE `site_visit` ("
    "`site_visit_id` varchar(36) NOT NULL,"
    "  `page_id` varchar(50) NOT NULL,"    
    "  `event_time` TIMESTAMP(6) NOT NULL,"
    "  `customer_id` varchar(50) NOT NULL,"
    "  PRIMARY KEY (`site_visit_id`)"
    ") ENGINE=InnoDB")
    
TABLES['site_visit_tag'] = (
    "CREATE TABLE `site_visit_tag` ("
    "`site_visit_tag_id` varchar(36) NOT NULL,"
    "`site_visit_id` varchar(36) NOT NULL,"
    "  `tag_name` varchar(50),"    
    "  `tag_value` varchar(50) ,"
    "  PRIMARY KEY (`site_visit_tag_id`)"
    ") ENGINE=InnoDB")    

TABLES['orders'] = (
    "CREATE TABLE `orders` ("
    "`order_id` varchar(36) NOT NULL,"
    "  `event_time` TIMESTAMP(6) NOT NULL,"
    "  `customer_id` varchar(50) NOT NULL,"
    "  `total_amount` decimal(10,2) NOT NULL,"    
    "  PRIMARY KEY (`order_id`)"
    ") ENGINE=InnoDB")


TABLES['image'] = (
    "CREATE TABLE `image` ("
    "`image_id` varchar(36) NOT NULL,"
    "  `event_time` TIMESTAMP(6) NOT NULL,"
    "  `customer_id` varchar(50) NOT NULL,"
    "  `camera_make` varchar(100),"    
    "  `camera_model` varchar(100),"    
    "  PRIMARY KEY (`image_id`)"
    ") ENGINE=InnoDB")
'''' connect to database'''
cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1')

cursor = cnx.cursor()


def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cnx.database = DB_NAME  

except Exception as e:
    print(type(e))
    print(e)
    input()
    #as err:

    err = mysql.connector.Error
    if e:
        create_database(cursor)
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)

for name, ddl in TABLES.items():
    try:
        print("Creating table {}: ".format(name), end='')
        cursor.execute(ddl)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.commit()
cnx.close()        