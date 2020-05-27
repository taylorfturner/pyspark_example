from decimal import Decimal
import logging 
import logging.config
import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from sqlalchemy import create_engine
from utils import xml_schema
import uuid
import yaml

class ETL():

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
        self.jdbc_params = self._inst_jdbc_params()
        
        try: 
            self.spark_session, self.sqlcontext = self._inst_context()

            self.logger.info('Class instantiation complete // {}'.format(self.etl_id))

        except Exception as e: 
            self.logger.error('{} // {}'.format(e, self.etl_id))

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
        
            db_df = self._get_mysql_data()
            db_df = db_df.select(['transaction_id', 'created_date', 'total_cost', 'ticket_quantity', 'reseller_location', 'commission_rate'])\
                        .withColumn("commission_amount", F.col('total_cost') * F.col('commission_rate'))
            
            csv_df = self._get_csv_data()
            csv_df = csv_df.withColumnRenamed('total_amount', 'total_cost')\
                            .withColumnRenamed('num_tickets', 'ticket_quantity')\
                            .withColumnRenamed('office_location', 'reseller_location')\
                            .select(['transaction_id', 'created_date', 'reseller_location', 'total_cost', 'ticket_quantity', 'commission_rate'])\
                            .withColumn('commission_amount', F.col('total_cost') * F.col('commission_rate'))

            xml_df = self._get_xml_data()
            xml_df = xml_df.select('')\
                            .withColumnRenamed('dateCreated', 'created_date')\
                            .withColumnRenamed('numberOfPurchasedtickets', 'ticket_quantity')\
                            .withColumnRenamed('totalAmount', 'total_cost')\
                            .withColumnRenamed('officeLocation', 'office_location')\
                            .withColumn('commission_amount', F.col('total_cost') * .10)
    
            df = db_df.union(csv_df).union(xml_df)

            db_df.unpersist()
            csv_df.unpersist()
            xml_df.unpersist()

            self.logger.info('Data Collection Complete // {}'.format(self.etl_id))

            return df
        
        except Exception as e: 
            self.logger.error('{} // {}'.format(e, self.etl_id))

    def run(self, df): 
        """ 
        Process and join the data as desired. 

        Key functionalities: 
            - Check for erroneous data
            - Support Update/Insert
            - Restartable if the job fails

        Arguments:
            df {Spark.DataFrame} -- Dataframe that is the result of a left join 
                of the three disparate data sources for the ticket sales. 
        """
        self.logger.info('Data Processing Start // {}'.format(self.etl_id))

        try: 
            #TODO: 1.) check for errorneous data values using self.__eroneous_data_value_check()
            #TODO: 2.) restartable if the job fails
            #TODO: 3.) UPSERT (INSERT / UPDATE) // hash each row in the dataset to then check in future runs if the data exists
            #   in the target data
            
            self.logger.info('Data Processing Complete // {}'.format(self.etl_id))

            return df

        except Exception as e: 
            self.logger.error('{} // {}'.format(e, self.etl_id))
        
    def put(self, df):
        """Writes data to final CSV file and stops spark session. 

        Arguments: 
            df[spark datafarme] -- spark dataframe that
        """
        try: 
            self.logger.info('Begin putting data to disk // {}'.format(self.etl_id))

            jdbc_params = self.jdbc_params
            jdbc_params['dbtable'] = 'toptal_final'

            df.write.format('jdbc').options(**jdbc_params)\
                .mode('overwrite')\
                .save()

            self.logger.info('Job complete // {}'.format(self.etl_id))

            self.spark_session.stop()

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
        """Instantiate the spark session, spark_context, and sqlcontext. 
            Return the sqlcontext object for reading data in the 
            `get()` method. 
        """
        try:
            spark_session = SparkSession\
                .builder\
                .config("spark.jars", "C:\spark\jars\mysql-connector-java-5.1.49-bin.jar") \
                .getOrCreate()

            sqlcontext = SQLContext(spark_session)
            self.logger.info('Spark Session Instantiated // {}'.format(self.etl_id))
    
        except Exception as e: 

            self.logger.error(e)

        return spark_session, sqlcontext

    def _inst_logger(self): 
        """Instantiate a logger and set the config for it.
        
        Return: 
            - logger object
        """
        current_dir = os.getcwd()

        with open(os.path.join(os.path.dirname(__file__), 'logger_conf.yaml'), 'r') as f:
            log_cfg = yaml.safe_load(f)

        logging.config.dictConfig(log_cfg)

        logger = logging.getLogger('etl')
        logger.setLevel('DEBUG')

        return logger

    def _read_creds(self, ): 
        """Retrieve all credentials and parameters required to instantiate 
            the JDBC connection to localhost MySQL database. 
        """
        current_dir = os.getcwd()

        with open(current_dir + '\\creds.yaml', 'r') as f:
            db_con_info = yaml.safe_load(f)

        return db_con_info['db_con_info']


    def _get_mysql_data(self): 
        """Hidden method for retrieving the mysql data for ticket sales.
        """
        self.logger.info('Begin mysql query // {}'.format(self.etl_id))

        try: 

            jdbc_options = self._inst_jdbc_params()
            jdbc_options['dbtable'] = 'toptal_sales'

            db_df = self.sqlcontext.read.format('jdbc').options(**jdbc_options).load()

            self.logger.info('MySQL data retrieved // {}'.format(self.etl_id))

            return db_df

        except Exception as e: 

            self.logger.error('Error retrieving data from mysql table {} // {}'.format(e, self.etl_id))

    def _get_xml_data(self): 
        file_rdd = self.sqlcontext.read.text('data/reseller_xml/*.xml', wholetext=True).rdd
        records_rdd = file_rdd.flatMap(parse_xml)
        df = records_rdd.toDF(my_schema)
        return df


    def _get_csv_data(self): 
        return self.sqlcontext.read.csv('data/reseller_csv/*.csv', header = True)

    def _inst_jdbc_params(self):
        """Hidden method for instantiating all the params for MySQL JDBC read operation. 

        Returns:
            options[dictionary] -- dictionary of parameters to pass to the pyspark JDBC 
                read from MySQL. 
        """
        db_con_info = self._read_creds()

        self.logger.warn('`useSSL` is set to `false` in the url for JDBC local testing')

        db_url = "jdbc:mysql://{}:{}/{}?useSSL=false".format(db_con_info['db_host'],\
                                                                db_con_info['db_port'],\
                                                                db_con_info['db_name'])
        options = {
            "url": db_url,
            "user": db_con_info['db_user'],
            "password": db_con_info['db_password'],
            "driver": "com.mysql.jdbc.Driver",
        }

        return options
        
    def _eroneous_data_value_check(self): 
        """Check for erroneous data values in a given dataframe
        """
        #TODO: 4.) check for nan/ null values
        pass


etl = ETL()
df = etl.get()
df = etl.run(df)
etl.put(df)