#!/usr/bin/ env python2.6
"""Retrieve ADP customer journey report

Usage:

    python2.6 report.py <configFilePath>
"""
import ConfigParser
import sys
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from ConfigParser import NoOptionError, NoSectionError, ParsingError, MissingSectionHeaderError


def get_config(file_path, section):
    # TODO: Define unit test for function
    """Get config from given section of file

    Args:
        file_path: The path for the .cfg file
        section: The section of the .cfg file containing the values required

    Returns:
        A dictionary of key value pairs corresponding to the entries in the .cfg file
    """
    try:
        config_dict = {}

        config = ConfigParser.ConfigParser()
        config.read(file_path)
        options = config.options(section)

        for option in options:
            if option == 'port':
                config_dict[option] = config.getint(section, option)
            else:
                config_dict[option] = config.get(section, option)

        return config_dict

    except (NoOptionError, NoSectionError, ParsingError, MissingSectionHeaderError, TypeError) as config_error:
        logging.error('Failed to extract config with error: %s', config_error)


def connect_database(host="127.0.0.1", port=27017, db="local"):
    """Connect to a Mongo database

    Args:
        host: The host name or ip address of the server where the mongo database resides
        port: The port the Mongo instance is listening on
        db: The name of the database to connect to

    Returns:
        A class of type Pymongo database connection
    """
    try:

        client = MongoClient(host, port)
        db_connection = client[locals()['db']]

        logging.info('Connected to database %s on %s', db, host)

        return db_connection

    except (ConnectionFailure, TypeError) as connection_error:
        logging.error('Failed to connect to database %s on host %s with error: %s', db, host, connection_error)


def connect_collection(db, collection_name):
    # TODO: Add doc string
    # TODO: Add error handling
    # TODO: Add unit test
    collection = db[locals()['collection_name']]

    logging.info('Connected to collection %s in database %s', collection_name, db)

    return collection


class GetReportFields(object):
    # TODO: Add doc string to class
    # TODO: Improve method doc strings
    # TODO: Add unit test to class
    # TODO: Add error handling to class
    # TODO: Convert to a dictionary handler class
    """Takes a python dictionary of report fields and provides methods for accessing subsets of the fields

    Args:
        field_dic: A dictionary of fields for the report with key value pairs used for filtering
        exclude: A list of fields of type list to exclude from the result

    Returns:
        A list of fields or a tuple of fields depending on the method called
    """

    def __init__(self, field_dic, exclude=[]):
        self._field_dic = field_dic
        self._exclude = exclude

    def order_fields(self):
        """Returns field aliases sorted by the order key"""
        fields = [d.get('alias') for d in sorted(self._field_dic, key=lambda k: (k.get('order')))
                  if d.get('display') == 1 and d.get('alias') not in self._exclude]
        return fields

    def version_fields(self):
        """Returns a list of unique pipeline fields that require version history to be maintained"""
        fields = set(
            [(d.get('alias'), d.get('pipeline'), d.get('latestVersion')) for d in self._field_dic if
             d.get('versioned') == 1 and d.get('alias') not in self._exclude])
        return fields

    def runtime_eligible_fields(self):
        """Returns a list of fields eligible for use in run time calculation"""
        fields = [d.get('alias') for d in self._field_dic if
                  d.get('eligibleForRunTimeCalculation') == 1 and d.get('alias') not in self._exclude]
        return fields

    def runtime_calculated_fields(self):
        """Returns a list of fields which are calculated at run time"""
        fields = [d.get('alias') for d in self._field_dic if
                  d.get('calculatedAtRunTime') == 1 and d.get('alias') not in self._exclude]
        return fields


def list_collections(database):
    # TODO: Add unit test
    # TODO: Add error handling
    # TODO: Add docstring
    """"""
    collections = [entry['name'] for entry in database.command('listCollections').get('cursor').get('firstBatch')]
    return collections


def list_filter(source_list, filter_list):
    """Returns a subset of source_list removing all elements from the list that contain a partial match
    on the name of the items in the filter_list

    Args:
        source_list: The list of items to be filtered
        filter_list: The list of items that should be exclude from the source_list

    Returns:
        A list of items that meet the criteria of the filter
    """
    remove_list = []
    for item in filter_list:
        temp_list = [x for x in source_list if x.lower().find(item.lower()) != -1]
        remove_list.extend(temp_list)
    result_list = set(source_list) - set(remove_list)
    return result_list


def add_index(db, indexes):
    """Adds an index to the collection and list of fields passed

    Args:
        db: The database to perform the operation in
        indexes:    A list of 2 item tuples, index 0 containing the collection name
                    and index 1 containing a list of fields

    Returns:
        list of operations performed
    """
    pass


def add_string_id_to_document(db, collections, excluded_collections=[]):
    # TODO: Add unit tests
    # TODO: Add error handling
    # TODO: Complete docstring
    """Adds string ID's to mongo documents for use in Aggregation framework $lookup stage
    and index them to work around the lack of support for type coercion in the mongo $lookup
    aggregation stage

    Args:
        db:

    Returns:
    """
    import bson
    from pymongo.errors import InvalidOperation

    collection_list = list_filter(collections, excluded_collections)

    for collection in collection_list:
        """Initialise a bulk operation for updating document"""
        bulk_op = db[locals()['collection']].initialize_unordered_bulk_op()

        document_cursor = db[locals()['collection']].find()
        for document in document_cursor:
            if document.get('_id') and document.get(locals()['collection'] + 'IDstr') is None:
                if isinstance(document.get('_id'), bson.objectid.ObjectId):
                    nid = str(document.get('_id'))
                    bulk_op.find({'_id': document.get('_id')}).update({'$set': {locals()['collection'] + 'IDstr': nid}})

        try:
            bulk_op.execute()
        except InvalidOperation, e:
            logging.debug('No string ids to be added to %s with message: ', e)


def main(cfg_file='./report.cfg'):
    # TODO: Add support for timestamped log files
    """Executes the report

    Args:
        cfg_file: A path to a .ini style config file

    Returns:
        An ADP customer advice journey report
    """
    try:
        import logging.config
        import aggregationPipelines.report_fields as report_fields
        import aggregationPipelines.get_latest_version as latest
        import aggregationPipelines.create_current_fact_find as fact_find
        import aggregationPipelines.create_version_master as master
        import aggregationPipelines.get_version_diff as diff
        import aggregationPipelines.get_final_report as final_report

        logging.config.fileConfig(cfg_file)

        """"Pull in config from config file"""
        """Source database details"""
        host = get_config(cfg_file, 'SourceDB')['host']
        port = get_config(cfg_file, 'SourceDB')['port']
        db = get_config(cfg_file, 'SourceDB')['db']
        collection = get_config(cfg_file, 'SourceDB')['collection']

        source_db = connect_database(host, port, db)
        source_collection = (source_db, collection)

        """Destination database details"""
        host = get_config(cfg_file, 'DestinationDB')['host']
        port = get_config(cfg_file, 'DestinationDB')['port']
        db = get_config(cfg_file, 'DestinationDB')['db']

        collection_stage = get_config(cfg_file, 'DestinationDB')['collection_stage']
        collection_master = get_config(cfg_file, 'DestinationDB')['collection_master']

        destination_db = connect_database(host, port, db)
        connection_collection_stage = (destination_db, collection_stage)
        connection_collection_master = (destination_db, collection_master)

        """Instantiate report fields class"""
        rf = GetReportFields(report_fields.fields)
        vf = rf.version_fields()

        """Generate aggregation pipelines for use in later stages"""
        M = master.create_project(vf)
        D = diff.create_project(vf)
        FF = fact_find.pipeline
        L = latest.pipeline
        F = final_report.create_project(vf)

        collections_source = list_collections(source_db)

        add_string_id_to_document(source_db, collections_source,
                                  ['event', 'audit', 'chunks', 'excel', 'lookup', 'files'])

    except KeyError as main_error:
        logging.error('Failed to execute main function with error: %s', main_error)


if __name__ == '__main__':

    if sys.argv[1:2] is None:
        import logging.config

        logging.config.fileConfig(sys.argv[1])

        main(sys.argv[1])
    else:
        print 'No config file passed in cmd line, please see help for more information'
