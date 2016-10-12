import report
import pytest

from pymongo import MongoClient


@pytest.fixture()
def mongo():
    db = MongoClient('127.0.0.1', 27017).local
    return db


def test_type_connect_database(mongo):
    expected_object = type(mongo)
    function_return = report.connect_database()
    assert type(function_return) == expected_object


def test_connection_connect_database(mongo):
    expected_object = mongo
    assert report.connect_database('127.0.0.1', 27017, 'local') == expected_object
