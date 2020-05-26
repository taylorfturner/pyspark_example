from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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