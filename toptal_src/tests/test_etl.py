import unittest
from toptal_src.etl import ETL 

class ToptalETLTesting(unittest.TestCase):

    def test_instantiation(self): 
        """Testing instantiation of the ETL class. 
        
        This ultimately tests the following: 
            - instantiating / configuration of logger 
            - instantiation of a `uuid.UUID4` as the "job's id"
            - instantiation and configuration of parameters for JDBC connection 
                to localhost MySQL 
            - instnatiation of SparkSession and SQLContext for interacting with spark cluster
                and with dataframes on spark cluster
        """
        etl = ETL()

        self.assertIsInstance(etl.jdbc_params, dict)
        self.assertIsInstance(etl.etl_id, str)
        self.assertIsInstance(etl.logger, logger)
        self.assertIsInstance(etl.sqlcontext, '<object_type>')
        self.assertIsInstance(etl.sqlcontext, '<object_type>')
    
    def test_etl_process(self): 
        """Run the complete process
        """
        etl = ETL() 
        df = etl.get() 
        df = etl.run(df)
        etl.put()

if __name__ == '__main__':
    unittest.main()