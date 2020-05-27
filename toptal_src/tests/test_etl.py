import unittest
from toptal_src.etl import ETL

#TODO: 80%+ coverage 

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
        # instantiate the class
        etl = ETL()

        # TODO: complete this 
        # make sure all class attributes are as they should be
        self.assertIsInstance(etl.jdbc_params, dict)
        self.assertIsInstance(etl.etl_id, str)
        self.assertIsInstance(etl.logger, logger)
        self.assertIsInstance(etl.sqlcontext, '<object_type>')
        self.assertIsInstance(etl.sqlcontext, '<object_type>')
    
    def test_etl_process(self): 
        """Run the complete process
        """
        #TODO: complete this 
        etl = ETL() 
        df = etl.get() 
        df = etl.run(df)
        etl.put()

if __name__ == '__main__':
    unittest.main()