from faker import Faker
import numpy as np
import pandas as pd
import yaml
import uuid
import random
from sqlalchemy import create_engine
import sys
from src.gen_data.utils import *

# instantiate Faker class
fake = Faker()
Faker.seed(0)

# specify lenght of dataframe 
size = 1000

# populate the dataframe 
df = pd.DataFrame(columns = ['transaction_id', 'seller', 'customer_first_name', 'customer_last_name', 'total_cost', 'ticket_quantity', 'sales_channel', 'commission_rate'])
df['seller'] = random_seller(size)
df['seller_id'] = [str(uuid.uuid4()) for _ in range(len(df.index))]
df['sales_channel'] = random_channel(size)
df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df.index))]
df['customer_first_name'] = random_names('first_name', size, fake)
df['customer_last_name'] = random_names('last_name', size, fake)
df['total_cost'] = np.random.randint(low=10, high = 100, size=size)
df['ticket_quantity'] = df['total_cost'] / 10 
df['reseller_location'] = random_addresses('city', size, fake)
df['created_date'] = random_dates(size, years_back = '-2y', fake)
df = df.apply(lambda x: commission_rate(x), axis = 1)

# write the dataframe to localhost MySQL 
db_con = create_engine('mysql+pymysql://root:Texas!234@localhost/hqc')
df.to_sql(name = 'toptal_sales', con = db_con, if_exists = 'replace')

# reseller CSV -- assumption here is we are looking back 2 years on daily data for 3 resellers 
reseller_df = pd.DataFrame(columns = ['transaction_id', 'event_name', 'num_tickets', 'total_amount', 'sales_channel', 'cust_first_name', 'cust_last_name', 'office_location', 'created_date'])
reseller_df['office_location'] = random_addresses('city', size, fake)
reseller_df['created_date'] = random_dates(size, '-2y', fake)
reseller_df['event_name'] = random_event(size, fake)
reseller_df['total_amount'] = np.random.randint(low=10, high = 100, size=size)
reseller_df['num_tickets'] = reseller_df['total_amount'] % 10
reseller_df['sales_channel'] = random_channel(size)
reseller_df['cust_first_name'] = random_names('first_name', size, fake)
reseller_df['cust_last_name'] = random_names('last_name', size, fake)
reseller_df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(reseller_df.index))]

for date in reseller_df['created_date']:
    for unique_reseller_id in random.sample(range(1, 100), 2):
        sub_df = reseller_df[reseller_df['created_date'] == date]
        sub_df.to_csv('data/reseller_csv/DailySales_{date}_{reseller_id}.csv'.format(date = date, reseller_id = unique_reseller_id), index = False)

# XML reseller dataframe -- https://pymotw.com/2/xml/etree/ElementTree/create.html
xml_df = pd.DataFrame(columns = ['transaction_id', 'event_name', 'num_tickets', 'total_amount', 'sales_channel', 'cust_first_name', 'cust_last_name', 'office_location', 'created_date'])
xml_df['office_location'] = random_addresses('city', size, fake)
xml_df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(xml_df.index))]
xml_df['sales_channel'] = random_channel(size)
xml_df['cust_first_name'] = random_names('first_name', size, fake)
xml_df['cust_last_name'] = random_names('last_name', size, fake)
xml_df['created_date'] = random_dates(size, '-2y', fake)
xml_df['event_name'] = random_event(size, fake)
xml_df['total_amount'] = np.random.randint(low=10, high = 100, size=size)
xml_df['num_tickets'] = xml_df['total_amount'] % 10

for row in xml_df.iterrows(): 
    
    row = row[1]
    
    if row['created_date'].month <= 6: 
        unique_reseller_id = '1'
    if row['created_date'].month > 6: 
        unique_reseller_id = '2'
    top = Element('xml')
    transaction = SubElement(top, 'transaction')
    transaction.set('date', str(row['created_date']))
    transaction.set('reseller_id', unique_reseller_id)
    transaction = append_sub_element(transaction, 'transaction_id', row['transaction_id'])
    transaction = append_sub_element(transaction, 'eventName', row['event_name'])
    transaction = append_sub_element(transaction, 'numberOfPurchasedTickets', str(row['num_tickets']))
    transaction = append_sub_element(transaction, 'totalAmount', str(row['total_amount']))
    transaction = append_sub_element(transaction, 'salesChannel', row['sales_channel'])
    transaction = append_sub_element(transaction, 'officeLocation', row['office_location'])
    transaction = append_sub_element(transaction, 'dateCreated', str(row['created_date']))
    customer = SubElement(transaction, 'customer')
    customer = append_sub_element(customer, 'first_name', 'joe')
    customer = append_sub_element(customer, 'last_name', 'nichols')
    tree = ElementTree(top)
    tree.write('data/reseller_xml/DailySales_{date}_{reseller_id}.xml'.format(date = row['created_date'], reseller_id = unique_reseller_id))