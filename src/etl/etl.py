from decimal import Decimal
import logging 
import logging.config
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from sqlalchemy import create_engine
import uuid
import yaml

class ETL(object): 
    """Class for ETL processing of ticket sale data for TopTal 
        technical interview challenge.
    """

    def __init__(self):
        """Instantiate the class for processing Sales data.

            Also set the following attributes of the class from hidden 
                methods in the class: 
                - logger object 
                - SQLContext object
                - ETL unique Job ID
        """
        self.logger = self._inst_logger()
        self.etl_id = self._inst_etl_id()
        
        try: 
            self.sqlcontext = self._inst_context()

            self.logger.info('Class instantiation complete // {}'.format(self.etl_id))

        except Exception as e: 
            self.logger.error('{} // {}'.format(e, self.etl_id)))

    def get(self): 
        """
        Retrieve all the data from the three data sources. 
            1.) MySQL localhost database where `toptal_sales` is the table name
            2.) CSV reseller data 
            3.) XML reseller data

        Return: 
            - Final dataframe for writing to disk. 
        """
        self.logger.info('Begin collecting the data // {}'.format(self.etl_id))
        
        try: 

            sc = self.sqlcontext.sparkContext
            sqlcontext = SQLContext(sc)

            #TODO: FIX THIS
            db_df = sqlcontext.read.format("jdbc").options(
                    url="jdbc:mysql://localhost:3306/hqc",
                    dbtable = "toptal_sales",
                    user="root",
                    password="Texas!234").load()

            csv_df = sqlcontext.read.csv('data\\reseller_csv\\*.csv')

            #TODO: put this in a good location
            schema = StructType([
                    StructField('date', IntegerType(), False),
                    StructField('reseller_id', IntegerType(), False),
                    StructField('transaction_id', StringType(), False),
                    StructField('eventName', StringType(), False),
                    StructField('numberOfPurchasedTickets', StringType(), False),
                    StructField('totalAmount', StringType(), False),
                    StructField('salesChannel', StringType(), False),
                    StructField('officeLocation', StringType(), False),
                    StructField('dateCreated', StringType(), False),
                    StructField('officeLocation', StringType(), False),
                    StructField('first_name', StringType(), False),
                    StructField('last_name', StringType(), False)
                ])

            xml_df = sc.read.format("com.databricks.spark.xml")\
                .option('rowTag', 'transaction')\
                .load('data/reseller_xml/*.xml', schema = schema)

            #TODO: left join all together --> df 

            self.logger.info('Data Collection Complete // {}'.format(self.etl_id))

            return df
        
        except Exception as e: 
            self.logger.error('{} // {}'.format(e, self.etl_id))

    def run(self, dataframe): 
        """ 
        Process and join the data as desired. 

        Key functionalities: 
            - Check for erroneous data
            - Support Update/Insert
            - Restartable if the job fails

        Arguments:
            dataframe {Spark.DataFrame} -- Dataframe that is the result of a left join 
                of the three disparate data sources for the ticket sales. 
        """
        # self.logger.info('Data Processing Start // {}'.format(self.etl_id))
        self.logger.warn('NOT IMPLEMENTED')
        pass
        #TODO: check for errorneous data values 
        # check for erroneous data 
        # restartable if the job fails

        """
        try: 
            self.logger.info('Data Processing Complete // {}'.format(self.etl_id))
        except Exception as e: 
            self.logger.error('{} // {}'.format(e, self.etl_id))
        """
    def put(self): 
        """Writes data to final CSV file and stops spark session. 
        """
        # repartition the file so that the output is one single file
        try: 
            self.logger.info('Begin putting data to disk // {}'.format(self.etl_id))

            self.df.repartition(1)\
                .write()\ # write to mysql database 
                .format('com.databricks.spark.csv')\
                .save('output\')

            self.logger.info('Job complete // {}'.format(self.etl_id))

            self.sqlcontext.stop()

        except Exception as e: 
            self.logger.error('{} // {}'.format(e, self.etl_id))

    def _inst_etl_id(self): 
        """Instantiate a unique UUID for the current run. 
        """
        try: 
            unique_job_id = uuid.uuid4()
            self.logger.info('ETL ID Generated // {}'.format(unique_job_id))
        
        except Exception as e:
            self.logger.error(e)

        return unique_job_id

    def _inst_context(self): 
        """
        Instantiate the spark session, spark_context, and sqlcontext. 
            Return the sqlcontext object for reading data in the 
            `get()` method. 
        """
        try:
            #TODO: setup class path -- .config('spark.driver.extraClassPath', 'testing')\ 
            session = SparkSession\
                    .builder\
                    .appName("toptal_sales")\
                    .config("spark.executor.extraClassPath", "mysql-connector-java-5.1.49")\
                    .config("spark.driver.extraClassPath", "mysql-connector-java-5.1.49")\
                    .getOrCreate()

            sc = session.sparkContext
            sqlcontext = SQLContext(sc)

            self.logger.info('Spark Session Instantiated // {}'.format(self.etl_id))
    
        except Exception as e: 

            self.logger.error(e)

        return sqlcontext

    def _inst_logger(self): 
        """
        Instantiate a logger and set the config for it.
        
        Return: 
            - logger object
        """
        current_dir = os.getcwd()

        with open(current_dir + '\\src\\etl\\logger_conf.yaml', 'r') as f:
            log_cfg = yaml.safe_load(f)

        logging.config.dictConfig(log_cfg)

        logger = logging.getLogger('etl')
        logger.setLevel('DEBUG')

        return logger