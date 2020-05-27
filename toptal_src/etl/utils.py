from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def parse_xml(rdd):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    results = []
    root = ET.fromstring(rdd[0])

    for b in root.findall('xml'):
        rec = []
        rec.append(b.attrib['id'])
        for e in ELEMENTS_TO_EXTRAT:
            if b.find(e) is None:
                rec.append(None)
                continue
            value = b.find(e).text
            if e == 'price':
                value = float(value)
            elif e == 'publish_date':
                value = datetime.strptime(value, '%Y-%m-%d')
            rec.append(value)
        results.append(rec)

    return results

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