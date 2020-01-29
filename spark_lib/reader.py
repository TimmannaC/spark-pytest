

def hive_reader(spark, query):
    return spark.sql(query)

