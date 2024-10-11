# Databricks notebook source
import logging
import datetime
import inspect
from pyspark.sql.types import StructType, StructField, StringType,TimestampType,IntegerType
import json
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

class CustomHandler(logging.Handler):

    def __init__(self):

        super().__init__()
        self.schema = StructType([
                    StructField("notebook_path", StringType(), True),
                    StructField("notebook_id", StringType(), True),
                    StructField("levelname", StringType(), True),
                    StructField("funcName", StringType(), True),
                    StructField("lineno", IntegerType(), True),
                    StructField("msg", StringType(), True),
                    StructField("created_timestamp", TimestampType(), True)
            ])

    def emit(self,record):

        if record:
            print('record levelname is :'record.levelname,record.funcName,record.lineno,record.msg)
            print('record funcName is :'record.funcName)
            print('record lineno is :'record.lineno)
            print('record msg is :'record.msg)
            # job_params = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
            # params = json.loads(job_params)
            # print("params", json.dumps(params, indent=4))
            # notebook_path = params["extraContext"]["notebook_path"]
            # notebook_id = params["extraContext"]["notebook_id"]
            # logs = []
            # logs.append((notebook_path,notebook_id,record.levelname,record.funcName,record.lineno,record.msg,datetime.datetime.utcnow()))
            # print(logs)
            # logs_df = spark.createDataFrame(data=logs, schema=self.schema)
            # logs_df.write.mode("append").saveAsTable("custom_logs")

 

def test_logs(logger):
    try:
        logger.debug('This is debug mode')
        logger.info('This is info mode')
        logger.warning('This is warning mode')
        out = 1/0
    except Exception as e:
        logger.error('This is error mode')
        logger.critical('This is critical mode')

if __name__ == '__main__':

   print(inspect.getsource(logging.LogRecord))
   print(inspect.getsource(logging.Handler))
#    logger = logging.getLogger('customLogs')
#    logger.setLevel(logging.DEBUG)
#    customhandler = CustomHandler()
#    logger.addHandler(customhandler)
#    test_logs(logger)

   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM custom_logs
