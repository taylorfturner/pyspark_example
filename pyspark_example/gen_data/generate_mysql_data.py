from faker import Faker
import numpy as np
import pandas as pd
import yaml
import uuid
import random
from sqlalchemy import create_engine
import sys
from utils import *

# instantiate Faker class
fake = Faker()
Faker.seed(0)

# specify lenght of dataframe 
size = 10

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
df['office_location'] = random_addresses('city', size, fake)
df['created_date'] = random_dates(size, '-2y', fake)
df = df.apply(lambda x: commission_rate(x), axis = 1)

# write the dataframe to localhost MySQL 
db_con = create_engine('mysql+pymysql://root:Texas!234@localhost/hqc')
df.to_sql(name = 'sales_data', con = db_con, if_exists = 'replace', index=False)