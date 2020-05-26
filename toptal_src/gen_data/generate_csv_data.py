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