import pytest
import findspark
findspark.init()
from pyspark.sql import SparkSession


def pytest_addoption(parser):
    parser.addoption(
        "--lwm", action="store", default="", help=" Please add the lower water mark"
    )
    parser.addoption(
        "--hwm", action="store", default="", help=" Please add the higher water mark"
    )


@pytest.fixture(scope="session")
def cmdopt(request):

    water_mark = {}
    water_mark['lwm'] = request.config.getoption("--lwm")
    water_mark['hwm'] = request.config.getoption("--hwm")

    return water_mark


@pytest.fixture(scope="session")
def spark_session(request):
    return SparkSession.builder.appName("pytest").enableHiveSupport().getOrCreate()


@pytest.fixture(scope="session")
def db_conf():

    db_prop = {'db': "default", 'tb': ["test_1", "test_2"]}

    return db_prop