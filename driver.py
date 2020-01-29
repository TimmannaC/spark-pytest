from pyspark.sql import SparkSession
from spark_lib.reader import hive_reader
import json

# spark-submit --deploy-mode client --master yarn --py-files spark_lib.zip driver.py

if __name__ == "__main__":

    spark = SparkSession.builder.appName("test").enableHiveSupport().getOrCreate()
    # spark.sparkContext.addPyFile("spark_lib.zip")
    with(open("conf.json")) as conf_json:
        json_conf = json.loads(conf_json.read())

    df_1 = hive_reader(spark, json_conf['step-1a'])
    df_1.show()

    df_2 = hive_reader(spark, json_conf['step-2a'])
    df_2.show()
