import pymongo
from pymongo import MongoClient
import configparser
from bson.objectid import ObjectId
import redis

# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your Mongo database.
def initialize():
    # this function will get called once, when the application starts.
    # this would be a good place to initalize your connection!
    # You might also want to connect to redis...
    config = configparser.ConfigParser()
    config.read('config.ini')
    client = MongoClient()
    db = client.cmps364

    global r
    global customers
    global products 
    global orders

    customers = client.cmps364_project2.customers
    products = client.cmps364_project2.products
    orders = client.cmps364_project2.orders

    r = redis.StrictRedis(host = 'redis-12663.c13.us-east-1-3.ec2.cloud.redislabs.com', port = 12663, password = 'OL9WT4niQkOazqF3MlzN1EABDKvIS6Lu',charset = 'utf-8', decode_responses = True)

#NEED TO CONNECT TO REDIS


def get_customers():
    customer = customers.find({})
    customerList = list()
    for singleCustomer in customer:
        customerList.append({'_id': singleCustomer['_id'], 'firstName': singleCustomer['firstName'], 'lastName':singleCustomer['lastName'], 'street': singleCustomer['street'], 'city':singleCustomer['city'], 'state': singleCustomer['state'], 'zip': singleCustomer['zip']})
    return customerList

def get_customer(id):
    customerID = ObjectId(id)
    singleCustomer = customers.find_one({'_id': customerID})
    return singleCustomer

def upsert_customer(customer):
    if '_id' in customer:
        tempCust = {'firstName':customer['firstName'],'lastName':customer['lastName'],'street':customer['street'],'city':customer['city'],'state':customer['state'], 'zip':customer['zip']}
        customers.update_one({'_id': ObjectId(customer['_id'])},{'$set':tempCust})
    else:
        customers.insert_one(customer)

def delete_customer(id):
    customerID = ObjectId(id)
    orders.delete_many({'customerId': customerID}) #CASCADE
    customers.delete_one({'_id': customerID})
    

def get_products():
    product = products.find({})
    productList = list()
    for singleProduct in product:
        productList.append({'_id': singleProduct['_id'], 'name': singleProduct['name'], 'price': singleProduct['price']})
    return productList


def get_product(id):
    productID = ObjectId(id)
    singleProduct = products.find_one({'_id': productID})
    return singleProduct

def upsert_product(product):
    if '_id' in product:
        tempProd = {'name':product['name'],'price':product['price']}
        products.update_one({'_id': ObjectId(product['_id'])},{'$set':tempProd})
    else:
        products.insert_one(product)

def delete_product(id):
    productID = ObjectId(id)
    products.remove({'_id': productID})
    orders.delete_many({'product': productID}) #CASCADE

def get_orders():
    order = orders.find({})
    orderList = list()
    for singleOrder in order:
        orderList.append({'_id':singleOrder['_id'],'customerId':singleOrder['customerId'], 'productId':singleOrder['productId'], 'date':singleOrder['date'], 'customer':get_customer(singleOrder['customerId']), 'product':get_product(singleOrder['productId']) })
    return orderList

def get_order(id):
    orderID = ObjectId(id)
    order = orders.find_one({'_id': orderID})
    orderInfo = {'_id':order['_id'], 'customerId':order['customerId'], 'productId':order['productId']}
    return orderInfo

def upsert_order(order):
    orders.insert_one(order)

def delete_order(id):
    orderID = ObjectId(id)
    orders.delete_one({'_id': orderID})

def customer_report(id):
    return None


    
# Pay close attention to what is being returned here.  singleCustomer product in the products
# list is a dictionary, that has all product attributes + last_order_date, total_sales, and 
# gross_revenue.  This is the function that needs to be use Redis as a cache.

# - When a product dictionary is computed, save it as a hash in Redis with the product's
#   ID as the key.  When preparing a product dictionary, before doing the computation, 
#   check if its already in redis!
def sales_report():
    r.flushall()
    salesReport = list()
    products = get_products()
    for singleProduct in products:
        product = r.hgetall(singleProduct['_id'])
        if r.exists(singleProduct['_id']):
            salesReport.append(product)
        else:
            orders = [o for o in get_orders() if o['product']['_id'] == singleProduct['_id']] 
            orders = sorted(orders, key=lambda k: k['date']) 
            if len(orders)>=1:
                singleProduct['last_order_date'] = orders[-1]['date']
            singleProduct['total_sales'] = len(orders)
            singleProduct['gross_revenue'] = singleProduct['price'] * singleProduct['total_sales']
            r.hmset(singleProduct['_id'], singleProduct)
            salesReport.append(singleProduct)
    return salesReport
