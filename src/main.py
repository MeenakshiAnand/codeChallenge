from __future__ import print_function
from mysql.connector import errorcode
import json
import uuid
import mysql.connector






SOURCE_FILE = '../input/data.txt'
DB_USER ='root'
DB_PASSWORD=''
DB_HOST = '127.0.0.1'
DB_NAME = 'shfly'
OP_FILE = '../output/output.txt'


# Create all tables

exec(open("createMysqlTable.py").read())

class Customer():
    """ Customer Class"""
    def __init__(self,e,cnx):
        self.customer_id = e["key"]
        self.event_time  = e["event_time"].replace("T"," ").replace("Z","")
        self.last_name = e["last_name"]
        self.adr_city = e["adr_city"]
        self.adr_state = e["adr_state"]
        self.cnx = cnx
    
    
    def dbRecExists(self):
        query = ("SELECT customer_id FROM customer "
         "WHERE customer_id = %s")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(query,(self.customer_id,))
        if cursor.rowcount == 0:
            cursor.close()
            return False
        else:
            cursor.close()
            return True
        
        
        
        
    def dbInsert(self):
        ins_query =("INSERT INTO customer "
               "(customer_id, event_time, last_name, adr_city, adr_state) "
               "VALUES (%s, %s, %s, %s, %s)")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(ins_query,(self.customer_id,self.event_time,self.last_name,self.adr_city,self.adr_state))
        cnx.commit()
    
    def dbUpd(self):
        
        upd_query =("UPDATE customer "
               "SET event_time = %s, last_name = %s, adr_city = %s, adr_state = %s "
               "WHERE customer_id = %s")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(upd_query,(self.event_time,self.last_name,self.adr_city,self.adr_state,self.customer_id))
        cnx.commit()
    
    def ingestData(self):
        
        if self.dbRecExists():
            self.dbUpd()
            print("Customer Record Updated")
        else:
            self.dbInsert()
            print("Customer Record Inserted")
            
    
        





class SiteVisit():
    
    def __init__(self,e,cnx):
        self.page_id = e["key"]
        self.event_time  = e["event_time"].replace("T"," ").replace("Z","")
        self.customer_id = e["customer_id"]
        self.tags = e["tags"]
        
    
    
    def dbRecExists(self):
        query = ("SELECT page_id FROM site_visit "
         "WHERE page_id = %s AND event_time = %s AND customer_id = %s")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(query,(self.page_id, self.event_time, self.customer_id))
        if cursor.rowcount == 0:
            cursor.close()
            return False
        else:
            cursor.close()
            return True
        
        
        
        
    def dbInsert(self):
        site_visit_id = str(uuid.uuid1())
        ins_query =("INSERT INTO site_visit "
               "(site_visit_id, page_id, event_time, customer_id) "
               "VALUES (%s, %s, %s, %s)")
        ins_query_tag =("INSERT INTO site_visit_tag "
               "(site_visit_tag_id,site_visit_id, tag_name, tag_value) "
               "VALUES (%s, %s, %s, %s)")               
        cursor = cnx.cursor(buffered=True)
        cursor.execute(ins_query,(site_visit_id,self.page_id,self.event_time,self.customer_id))
        
        if self.tags:
            for tag in self.tags:
                site_visit_tag_id = str(uuid.uuid1())
                for tag_name,tag_value in tag.items():
                    cursor.execute(ins_query_tag,(site_visit_tag_id,site_visit_id,tag_name,tag_value))
        
        cnx.commit()
    
    
    def ingestData(self):
        
        if self.dbRecExists():
            #do nothing
            print(" Record Exists")
        else:
            self.dbInsert()
            print(" Record Inserted")
            
            
            
            
class Order():
    
    def __init__(self,e,cnx):
        self.order_id = e["key"]
        self.event_time  = e["event_time"].replace("T"," ").replace("Z","")
        self.customer_id = e["customer_id"]
        self.total_amount = e["total_amount"].replace("USD"," ").replace(" ","")

    
    def dbRecExists(self):
        query = ("SELECT order_id FROM orders "
         "WHERE order_id = %s")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(query,(self.order_id,))
        if cursor.rowcount == 0:
            cursor.close()
            return False
        else:
            cursor.close()
            return True
        
        
        
        
    def dbInsert(self):
        ins_query =("INSERT INTO orders "
               "(order_id, event_time, customer_id, total_amount) "
               "VALUES (%s, %s, %s, %s)")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(ins_query,(self.order_id,self.event_time,self.customer_id,self.total_amount))
        cnx.commit()
    
    def dbUpd(self):
        
        upd_query =("UPDATE orders "
               "SET event_time = %s, customer_id = %s, total_amount = %s "
               "WHERE order_id = %s AND event_time < %s")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(upd_query,(self.event_time,self.customer_id,self.total_amount,self.order_id,self.event_time))
        cnx.commit()
    
    def ingestData(self):
        
        if self.dbRecExists():
            self.dbUpd()
            print(" Record Updated")
        else:
            self.dbInsert()
            print(" Record Inserted")
            

class Image():
    
    def __init__(self,e,cnx):
        self.image_id = e["key"]
        self.event_time  = e["event_time"].replace("T"," ").replace("Z","")
        self.customer_id = e["customer_id"]
        self.camera_make = e["camera_make"]
        self.camera_model = e["camera_model"]
        

    
    def dbRecExists(self):
        query = ("SELECT image_id FROM image "
         "WHERE image_id = %s")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(query,(self.image_id,))
        if cursor.rowcount == 0:
            cursor.close()
            return False
        else:
            cursor.close()
            return True
        
        
        
        
    def dbInsert(self):
        ins_query =("INSERT INTO image "
               "(image_id, event_time, customer_id, camera_make,camera_model) "
               "VALUES (%s, %s, %s, %s, %s)")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(ins_query,(self.image_id,self.event_time,self.customer_id,self.camera_make,self.camera_model))
        cnx.commit()
    
    def dbUpd(self):
        
        upd_query =("UPDATE image "
               "SET event_time = %s, customer_id = %s, camera_make = %s, camera_model = %s "
               "WHERE image_id = %s AND event_time < %s")
        cursor = cnx.cursor(buffered=True)
        cursor.execute(upd_query,(self.event_time,self.customer_id,self.camera_make,self.camera_model,self.image_id,self.event_time))
        cnx.commit()
    
    def ingestData(self):
        
        if self.dbRecExists():
            self.dbUpd()
            print(" Record Updated")
        else:
            self.dbInsert()
            print(" Record Inserted")
            

            
    
        
        

def Ingest(e, cnx):
    
    if e["type"] == "CUSTOMER":
        c = Customer(e,cnx)
        c.ingestData()

    elif e["type"] == "SITE_VISIT":
        c = SiteVisit(e,cnx)            
        c.ingestData()

    elif e["type"] == "ORDER":
        c = Order(e,cnx)
    
        c.ingestData()
        
    elif e["type"] == "IMAGE":
        c = Image(e,cnx)
        c.ingestData()




def TopXSimpleLTVCustomers(x, cnx):
    
    sel_query = ("select " 
                "site_visit.customer_id,"
                "(max_week_in_data - first_visit_week_no + 1) as customer_site_age,"
                "coalesce(total_amount,0) customer_total_spent,"
                "coalesce(total_amount,0) / (max_week_in_data - first_visit_week_no + 1) as avg_weekly_spent,"
                "coalesce(total_amount,0) / (max_week_in_data - first_visit_week_no + 1) *52*10 as CustomerSimpleLtv "
                "from "
                    "(select "
                    "customer_id,"
                    "count(*) as total_site_visit,"
                    "year(min(event_time))*100 + week(min(event_time)) as first_visit_week_no "
                    "from site_visit "
                    "group by customer_id "
                    ")site_visit "
                "cross join "
                    "("
                    "select year(max(event_time))*100 + week(max(event_time)) as max_week_in_data from  site_visit"
                    ")max_event_week "
                "left outer join"
                    "(select customer_id,sum(total_amount) as total_amount "
                    "from orders "
                    "group by customer_id) ord "
                "on site_visit.customer_id = ord.customer_id "
                "order by  5 desc "
                "limit %s"
                  )

    cursor = cnx.cursor(buffered=True)
    cursor.execute(sel_query,(x,))
    
    customerLst = []
    for op in cursor:
        customerLst.append(op[0])
    return customerLst

if __name__ == "__main__":
    
    top_cust_cnt = int(input("Enter Number of top customer to be written to Output File"))
  
    #Read Event Data
    with open(SOURCE_FILE) as fl:
        events = json.load(fl)
        fl.close()
     
    cnx = mysql.connector.connect(user=DB_USER, password= DB_PASSWORD,host=DB_HOST,  database=DB_NAME)
    
    #Ingest event data
    for e in events:
        Ingest(e, cnx)
    
    
    
    top_cust_list = TopXSimpleLTVCustomers(top_cust_cnt,cnx)
    cnx.close()
    
    fl = open(OP_FILE,'w') 
    i = 1
    for cus in top_cust_list:
        fl.write("Customer Rank: "+ repr(i) +" Customer Id: "+ cus + "\n")
        i = i + 1
    fl.close()