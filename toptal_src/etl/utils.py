from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import xml.etree.ElementTree as ET

def parse_xml_string(rdd): 

	rec = []

	tree = ET.fromstring(rdd[0])

	for iter_xml in xml_string.getiterator(): 
		
		print (iter_xml, iter_xml.tag, iter_xml.text)

		rec.append(iter_xml.text)

	return rec

def set_schema():
    """
    Define the schema for the DataFrame
    """
    schema_list = []
    
    for c in ['transaction_id', 'first_name', 'last_name', 'dateCreated', 'officeLocation', 'salesChannel', 'totalAmount', 'numberOfPurchasedTickets', 'eventName', 'date', 'reseller_id']:

            schema_list.append(StructField(c, StringType(), True))
    
    return StructType(schema_list)

xml_schema = StructType([
        StructField('date', IntegerType(), False),
        StructField('reseller_id', IntegerType(), False),
        StructField('transaction_id', StringType(), False),
        StructField('eventName', StringType(), False),
        StructField('numberOfPurchasedTickets', StringType(), False),
        StructField('totalAmount', StringType(), False),
        StructField('salesChannel', StringType(), False),
        StructField('officeLocation', StringType(), False),
        StructField('dateCreated', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False)
    ])