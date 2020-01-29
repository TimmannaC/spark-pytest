
"""
to run the pytest with parameters.
pytest -s --lwm  "2019-11-01 23:59:59" --hwm "2019-12-01 23:59:59" pytest-driver.py

"""


def test_build_sql_query(spark_session, db_conf, cmdopt):
    print(spark_session.version)
    print(db_conf)
    print(cmdopt)


def test_table_count(spark_session, db_conf, cmdopt):
    db_name = db_conf['db']
    tables_list = db_conf['tb']

    for table in tables_list:
        hive_query = "SELECT *FROM " + table + " WHERE insert_date BETWEEN " + cmdopt['lwm'] + "AND" + cmdopt['hwm']
        print(hive_query)
        hive_count = spark_session.read.option("query", hive_query).load().count()
        print("HIVE table count is      :       " + hive_count)


if __name__ == "__main__":
    print("Inside main !!")
