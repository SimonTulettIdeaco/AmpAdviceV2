import report
import mongomock


# import pytest


# @pytest.fixture()
# def mongo(mongo_server):
#     db =


# def test_type_connect_database(mongodb):
#     expected_object = type(mongodb.MongoClient.local)
#     function_return = report.connect_database()
#     assert type(function_return) == expected_object
#
#
# def test_result_connect_database(mongo_server):
#     expected_object = mongo_server.api.local
#     assert report.connect_database('127.0.0.1', 27017, 'local') == expected_object

def test_person(mongodb):
    db = mongodb['person']
    db.person.insert({'firstName': 'Simon', 'lastName': 'Tulett'})
    assert db.test.find_one()['firstName'] == 'Simon'

