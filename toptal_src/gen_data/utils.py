from xml.etree.ElementTree import ElementTree, Element, SubElement, Comment, tostring
import numpy as np 

def random_event(size, fake):
    event_names = []
    events = getattr(fake, 'company')
    for _ in range(size): 
        event_names.append(events())
    return event_names

def random_names(name_type, size, fake): 
    result_names = []
    names = getattr(fake, name_type)
    for _ in range(size): 
        result_names.append(names())
    return result_names

def random_addresses(type, size, fake):
    result_addresses = []
    addresses = getattr(fake, type)
    for _ in range(size): 
        result_addresses.append(addresses())
    return result_addresses

def random_seller(size): 
    sellers = ['organizer', 'reseller']
    return np.random.choice(sellers, size=size)

def random_channel(size): 
    sales_channel = ['office', 'web', 'mobile']
    return np.random.choice(sales_channel, size=size)

def random_dates(size, years_back, fake): 
    dates_list = []
    for _ in range(size): 
        dates_list.append(fake.date_between(start_date = years_back))
    return dates_list

def append_sub_element(top_var, element_name, element_text): 
    child = SubElement(top_var, element_name)
    child.text = element_text
    return top_var

def commission_rate(row): 
    if row['seller'] == 'reseller': 
        row['commission_rate'] = .10
    else: 
        row['commission_rate'] = 0 
    return row