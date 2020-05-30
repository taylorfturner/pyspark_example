import logging 
import logging.config
import os
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql import functions as F
from sqlalchemy import create_engine
from utils import xml_schema, set_schema
import uuid
import yaml
import xml.etree.ElementTree as ET

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

            self.logger.info('ETL Class instantiation complete // {}'.format(self.etl_id))

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
        try: 
        
            self.logger.info('Begin MySQL data retrieval // {}'.format(self.etl_id))
            db_df = self._get_mysql_data()
            db_df = db_df.withColumn("commission_amount", F.col('total_cost') * F.col('commission_rate'))\
                            .withColumnRenamed('office_location', 'reseller_location')\
                            .select(['transaction_id', 'created_date', 'total_cost', 'ticket_quantity', 'reseller_location', 'commission_amount'])\
                            .repartition(7)
            
            self.logger.info('Begin CSV data retrieval // {}'.format(self.etl_id))
            csv_df = self._get_csv_data()
            csv_df = csv_df.withColumnRenamed('total_amount', 'total_cost')\
                            .withColumnRenamed('num_tickets', 'ticket_quantity')\
                            .withColumnRenamed('office_location', 'reseller_location')\
                            .withColumn('commission_amount', F.col('total_cost') * F.col('commission_rate'))\
                            .select(['transaction_id', 'created_date', 'total_cost', 'ticket_quantity', 'reseller_location', 'commission_amount'])\
                            .repartition(7)

            self.logger.info('Begin XML data retrieval // {}'.format(self.etl_id))
            xml_df = self._get_xml_data()
            xml_df = xml_df.withColumnRenamed('dateCreated', 'created_date')\
                            .withColumnRenamed('numberOfPurchasedtickets', 'ticket_quantity')\
                            .withColumnRenamed('totalAmount', 'total_cost')\
                            .withColumnRenamed('officeLocation', 'reseller_location')\
                            .withColumn('commission_amount', F.col('total_cost') * .10)\
                            .select(['transaction_id', 'created_date', 'total_cost', 'ticket_quantity', 'reseller_location', 'commission_amount'])\
                            .repartition(7)

            df = db_df.union(csv_df)
            df = df.union(xml_df)

            db_df.unpersist()
            csv_df.unpersist()
            xml_df.unpersist()

            self.logger.info('Data Collection Complete // {}'.format(self.etl_id))
            
            self._count_is_equal(df)

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
            
            test_df = df.na.drop()
            test_df.count() == df.count()

            jdbc_options = self._inst_jdbc_params()
            jdbc_options['dbtable'] = 'toptal_final'

            target_df = self.sqlcontext.read.format('jdbc').options(**jdbc_options).load()

            coalesce_cols = [column for column in target_df.columns if column not in ['transaction_id', 'process_date']]
            
            param_df = df
            df = df.alias('a').join(
                    target_df.alias('b'), ['transaction_id'], how='outer'
                ).select('transaction_id', 
                    *(F.coalesce('b.' + col, 'a.' + col).alias(col) for col in coalesce_cols)
                ).distinct()

            insert_row_count = (df - param_df)
            if insert_row_count > 0: 
                self.logger.info('Inserting {} new rows in target dataframe // {}'.format(insert_row_count, self.etl_id))
            
            self.logger.info('Data Processing Complete // {}'.format(self.etl_id))

            test_df.unpersist()

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

            jdbc_params = self._inst_jdbc_params()
            jdbc_params['dbtable'] = 'toptal_final'

            df = df.withColumn('process_date', F.current_date())

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
            self.logger.error('{} // {}'.format(e, self.etl_id))

        return unique_job_id

    def _inst_context(self): 
        """Instantiate the spark session, spark_context, and sqlcontext. 
            Return the sqlcontext object for reading data in the 
            `get()` method. 
        """
        try:
            spark_session = SparkSession\
                .builder\
                .config("spark.jars", "C:\spark\jars\*.jar")\
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")\
                .getOrCreate()

            sqlContext = SQLContext(sparkContext=spark_session.sparkContext,\
                                    sparkSession=spark_session)
            self.logger.info('Spark Session Instantiated // {}'.format(self.etl_id))
    
        except Exception as e:
            self.logger.error(e)

        return spark_session, sqlContext

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

    def _read_creds(self): 
        """Retrieve all credentials and parameters required to instantiate 
            the JDBC connection to localhost MySQL database. 
        """
        current_dir = os.getcwd()

        with open(current_dir + '\\creds.yaml', 'r') as f:
            db_con_info = yaml.safe_load(f)

        return db_con_info['db_con_info']


    def _get_mysql_data(self): 
        jdbc_options = self._inst_jdbc_params()
        jdbc_options['dbtable'] = 'toptal_sales'

        df = self.sqlcontext.read.format('jdbc').options(**jdbc_options).load()

        self._is_dataframe('MySQL', df)
        self._is_populated('MySQL', df)

        return df


    def _get_xml_data(self): 
        df = self.sqlcontext.read.format("xml")\
                    .schema(set_schema())\
                    .options(rowTag='transaction')\
                    .load("data/reseller_xml/*.xml")
        
        self._is_dataframe('xml', df)
        self._is_populated('xml', df)

        return df

    def _get_csv_data(self): 
        df = self.sqlcontext.read.csv('data/reseller_csv/*.csv', header = True)

        self._is_dataframe('csv', df)
        self._is_populated('csv', df)

        return df

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

    def _is_populated(self, data_type, df): 
        if df.count() >= 1: 
            pass
        else: 
            self.logger.error('{} DataFrame is not populated // {}'.format(data_type, self.etl_id))
    
    def _is_dataframe(self, data_type, df): 
        if isinstance(df, DataFrame): 
            self.logger.info('Complete {} data retrieval // {}'.format(data_type, self.etl_id))
        else:
            self.logger.error('{} `df` object is not a dataframe // {}'.format(data_type, self.etl_id))

    def _count_is_equal(self,df): 
        if df.count() == 40:
            pass
        else: 
            self.logger.error('`df.count()` is not correct // {}'.format(self.etl_id))