#!/usr/bin/ env python2.6
"""Retrieve ADP customer journey report

Usage:

    python2.6 report.py <configFilePath>
"""
import ConfigParser
import logging
import sys
import re
import bson
import pytz

from itertools import izip_longest
from ConfigParser import NoOptionError, NoSectionError, ParsingError, MissingSectionHeaderError
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, InvalidOperation, BulkWriteError


# TODO: Define a logging function to log progress
# TODO: Include the additions made to v1 of the report since v2 was started listed below
"""

"""


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
    # TODO: Address mutable default value
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
        fields = [{'alias': d.get('alias'), 'pipeline': d.get('pipeline')} for d in
                  sorted(self._field_dic, key=lambda k: (k.get('order')))
                  if d.get('display') == 1 and d.get('alias') not in self._exclude]
        return fields

    def version_fields(self):
        """Returns a list of unique pipeline fields that require version history to be maintained"""
        fields = [{'alias': d.get('alias'), 'pipeline': d.get('pipeline'), 'latestVersion': d.get('latestVersion')} for
                  d in self._field_dic if
                  d.get('versioned') == 1 and d.get('alias') not in self._exclude]
        return fields

    def non_version_fields(self):
        """Returns a list of unique pipeline fields that require version history to be maintained"""
        fields = [{'alias': d.get('alias'), 'pipeline': d.get('pipeline')} for
                  d in self._field_dic if
                  d.get('versioned') == 0 and d.get('calculatedAtRunTime') == 0 and d.get('alias') not in self._exclude]
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
    """List all collections in the mongo database passed

    Args:
        database: A mongo database on which a listCollections cmd can be called

    Returns:
        A list of collections
    """
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


def parse_index_config(indexes):
    """Parses a config string of indexes into a list of tuples with nested list of indexes

    Args:
        indexes: string containing the list of indexes to parse

    Returns:
        A list of tuples
    """

    indexes_paren_split = re.findall('\(.*?\)', ''.join(indexes.split())[1:-1])

    indexes_bracket_split = (x.partition(',') for x in indexes_paren_split)
    replace_chars = ['(', ')', '[', ']']

    result = []

    for item in indexes_bracket_split:
        f, _, l = item
        nl = ''
        nf = ''
        for fc, lc in izip_longest(f, l):
            if lc not in replace_chars and lc is not None:
                nl += lc
            if fc not in replace_chars and fc is not None:
                nf += fc

        result.append((nf, nl.split(',')))

    return result


def add_index(db, indexes):
    # TODO: Add unit tests
    # TODO: Add error handling
    # TODO: Need to load test
    """Adds an index to the collection and list of fields passed

    Args:
        db: The database to perform the operation in
        indexes:    A list of 2 item tuples, tuple index 0 containing the collection name
                    and index 1 containing a list of fields

    Returns:
        list of operations performed
    """
    for c, fi in indexes:
        for f in fi:
            db[locals()['goals_cursor']].create_index(locals()['f'])


def add_string_id_to_document(db, collections, excluded_collections=[]):
    # TODO: Add unit tests
    # TODO: Add error handling
    # TODO: Complete docstring
    # TODO: Need to load test
    # TODO: Need to add indexes to string ID's
    """Adds string ID's to mongo documents for use in Aggregation framework $lookup stage
    and index them to work around the lack of support for type coercion in the mongo $lookup
    aggregation stage

    Args:
        db: Database to perform the operation in
        collections: List of collections to perform the operations against
        excluded_collections: List of collections to exclude if required

    Returns:
        No return it performs a bulk operation on the database passed
    """
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


def run_aggregate(collection, pipeline, operation):
    # TODO: Add unit tests
    # TODO: Add error handling
    # TODO: Add examples calls to docstring
    # TODO: Catch pymongo OperationFailure
    """Runs a Mongo aggregation framework query

    Args:
        collection: A pymongo collection object
        pipeline: A Mongo aggregation framework query
        operation:   Whether the aggregate query is intended to return a result or generate a new Mongo collection
                    via the $out pipeline stage

    Returns:
        A pymongo cursor object or no result in the case of the use of the $out pipeline stage
    """
    if operation == 'create':
        collection.aggregate(pipeline)
    else:
        return collection.aggregate(pipeline, cursor={})


def move_collection(source_db, destination_db, source_collection, destination_collection):
    # TODO: Add unit tests
    # TODO: Expand doc string to include example calls and results
    """Move a collection from one database to another using the admin capability of MongoDB

    Args:
        source_db: The database that contains the collection to be moved
        source_collection: The collection to be moved
        destination_db: The database the collection will be moved to
        destination_collection: The name of the destination collection

    Returns:
        Returns 1 if operation successful
    """

    try:
        MongoClient().admin.command("renameCollection",
                                    **{"renameCollection": '{0}.{1}'.format(locals()['source_db'],
                                                                            locals()['source_collection']),
                                       "to": '{0}.{1}'.format(locals()['destination_db'],
                                                              locals()['destination_collection']),
                                       "dropTarget": True})
        return 1
    except OperationFailure, e:
        raise e


def convert_dates(date_value, fmt='%Y-%m-%d %H:%M:%S'):
    # TODO: Test with a real recent fs.files record from UAT
    # TODO: Add unit tests
    # TODO: Add error handling
    """Takes a value not necessarily of type datetime and attempts to parse to a valid local datetime

    Args:
        date_value: The date value to parse, supported types are long, datetime, bson.objectid.ObjectId
        fmt: A format supported by strftime as per the following documentation http://strftime.org/

    Returns:
        A value of type datetime formatted as per the value passed to _fmt
    """
    tz_utc = pytz.utc
    tz_local = pytz.timezone('Australia/Sydney')

    if isinstance(date_value, long):
        date_converted = tz_local.localize(datetime.fromtimestamp(date_value / 1000))
    # elif isinstance(date_value, bson.objectid.ObjectId):
    #     date_converted = date_value.generation_time.replace(tzinfo=tz_utc)
    elif isinstance(date_value, datetime):
        if date_value.tzinfo is None:
            date_converted = tz_utc.localize(date_value)
        else:
            raise ValueError('Cannot convert timezone aware dates')
    elif date_value:
        return date_value
    else:
        date_converted = 0

    if date_converted != 0:
        ltz_date_time = date_converted.astimezone(tz_local)
        ltz_string_time = ltz_date_time.strftime(fmt)
        return datetime.strptime(ltz_string_time, fmt)


class DateTimes(object):
    # TODO: Add docstring
    # TODO: Add unit tests
    # TODO: Add error handling
    # TODO: Considering refactoring, class has become multi purpose
    # TODO: Suppress stack trace on raise ValueError
    """Creates UTC and LTZ or Unix EPOCH date time values

    Args:
        fmt_file: The datetime format to return the string value date from the get_string_date_ltz method

    Returns:
        """
    _fmt = '%Y-%m-%d %H:%M:%S'
    _tz_utc = pytz.utc
    _tz_local = pytz.timezone('Australia/Sydney')

    def __init__(self, fmt_file='%Y-%m-%d_%H%M%S'):
        self._current_date_time = datetime.now()
        self._current_date_time_utc = self._tz_local.localize(self._current_date_time, is_dst=True).astimezone(
            self._tz_utc)
        self._current_date_time_ltz = self._tz_local.localize(self._current_date_time, is_dst=True)
        self._zero_date_time_utc = datetime.fromtimestamp(0, self._tz_utc)
        self._zero_date_time_ltz = datetime.fromtimestamp(0, self._tz_local)
        self._fmt_file = fmt_file

    def get_date_no_time(self):
        """
        Returns:
            A tz naive zero hour, minute, second, microsecond date
        """
        return self._current_date_time.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_date_no_time_ltz(self):
        """
        Returns:
            A local timezone zero hour, minute, second, microsecond date
        """
        return self._current_date_time_ltz.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_date_no_time_utc(self):
        """
        Returns:
            A utc zero hour, minute, second, microsecond date
        """
        return self._current_date_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_ms_time_utc(self):
        """
        Returns:
            A unix utc epoch ms time using the datetime value generated at class instantiation
        """
        diff = self._current_date_time_utc - self._zero_date_time_utc
        return (diff.days * 86400000) + (diff.seconds * 1000) + (diff.microseconds / 1000)

    def get_string_date_ltz(self):
        """
        Returns:
            A date of type string based on the current local date matching the format passed to fmt_file_name
            at class instantiation
        """
        return self._current_date_time_ltz.strftime(self._fmt_file)


def insert_missing_master_records(source_db_cursor, destination_coll, versioned_fields, non_versioned_fields, bulk_op,
                                  ts):
    # TODO: Use correct TS from a timestamp function
    # TODO: Add docstring
    # TODO: Add error handling
    # TODO: Add unit testing
    # TODO: Generate diff report,from function and write to file, store file location in report.cfg
    # TODO: Correctly configure logging
    # TODO: Missing records cannot use the version diff cursor to create the missing records, because it will error on $size operation if the field doesn't exist
    """Inserts any records that exist in the latest version of data that don't exist in the Master collection
    with the correct version based field formatting, array of objects for versioned fields and as is for non
    versioned fields

    Args:

    Returns:

    """

    for record in source_db_cursor:

        versioned_fields_pipeline = set([field.get('pipeline') for field in versioned_fields])
        non_versioned_fields_pipeline = set([field.get('pipeline') for field in non_versioned_fields])
        new_dic = {}

        if destination_coll.find({'_id': record.get('_id')}).limit(1).count() != 1:
            new_dic.update({'_id': record.get('_id')})
            for k, v in record.items():
                if k in versioned_fields_pipeline and v is not None:
                    new_dic.update({k: [{'vid': 1, 'data': v, 'ts': ts}]})
                if k in non_versioned_fields_pipeline:
                    new_dic.update({k: v})

            bulk_op.insert(new_dic)

    try:
        bulk_op.execute()
    except (InvalidOperation, BulkWriteError) as e:
        logging.debug(e)


def insert_missing_master_fields(source_db_cursor, versioned_fields, non_versioned_fields, bulk_op):
    """Iterates the master report collection and adds fields which are missing with the correct type,
    empty array for versioned fields and as is for non versioned fields

    Args:

    Returns:

    """
    versioned_fields_pipeline = set([field.get('pipeline') for field in versioned_fields])
    non_versioned_fields_pipeline = set([field.get('pipeline') for field in non_versioned_fields])

    for record in source_db_cursor:
        update_dic = {'$set': {}}
        for vf, uvf in izip_longest(versioned_fields_pipeline, non_versioned_fields_pipeline):
            if vf is not None and record.get(vf, 'KeyMissing') == 'KeyMissing':
                update_dic['$set'].update({vf: []})
            if uvf is not None and record.get(uvf, 'KeyMissing') == 'KeyMissing':
                update_dic['$set'].update({uvf: None})

        if len(update_dic['$set']) > 0:
            bulk_op.find({'_id': record.get('_id')}).update(update_dic)

    try:
        bulk_op.execute()
    except (InvalidOperation, BulkWriteError) as e:
        logging.debug(e)


def apply_diff_to_master(source_db_cursor, bulk_op, valid_fields, versioned_fields, ts):
    """Updates fields with the latest data either by appending to an array with the latest version of with
    an in place replace of the data

    Args:

    Returns:

    """
    versioned_fields_pipeline = set([field.get('pipeline') for field in versioned_fields])
    valid_fields_pipeline = set([field.get('pipeline') for field in valid_fields])

    for document in source_db_cursor:
        doc_id = document['_id']
        for k, v in document.iteritems():
            if k in valid_fields_pipeline and isinstance(v, list) and len(v) > 0:
                for dic in v:
                    bulk_op.find({"_id": doc_id}).update(
                        {"$push": {
                            k:
                                {
                                    'vid': dic['vid'],
                                    'data': dic['data'],
                                    'ts': ts
                                }
                        }})
            elif k in valid_fields_pipeline and k not in versioned_fields_pipeline:
                bulk_op.find({"_id": doc_id}).update({"$set": {k: v}})
    try:
        bulk_op.execute()
    except (InvalidOperation, BulkWriteError) as e:
        logging.debug(e)


def main(cfg_file='./report.cfg'):
    # TODO: Add support for timestamped log files
    # TODO: Rename params to more logical names
    # TODO: Ensure db param naming is consistent
    """Executes the report

    Args:
        cfg_file: A path to a .ini style config file

    Returns:
        An ADP customer advice journey report
    """
    # try:
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
    db_name = get_config(cfg_file, 'SourceDB')['db']
    collection_source = get_config(cfg_file, 'SourceDB')['collection_main']
    collection_factfind = get_config(cfg_file, 'SourceDB')['collection_factfind']

    source_db = connect_database(host, port, db_name)
    person_collection = connect_collection(source_db, collection_source)
    fact_find_audit_collection = connect_collection(source_db, collection_factfind)

    """Destination database details"""
    host_destination = get_config(cfg_file, 'DestinationDB')['host']
    port_destination = get_config(cfg_file, 'DestinationDB')['port']
    db_name_destination = get_config(cfg_file, 'DestinationDB')['db']

    collection_stage = get_config(cfg_file, 'DestinationDB')['collection_stage']
    collection_master = get_config(cfg_file, 'DestinationDB')['collection_master']

    destination_db = connect_database(host_destination, port_destination, db_name_destination)

    connection_collection_stage = connect_collection(destination_db, collection_stage)

    connection_collection_master = connect_collection(destination_db, collection_master)

    """Instantiate report fields class"""
    report_fields = GetReportFields(report_fields.fields)
    version_fields = report_fields.version_fields()
    non_version_fields = report_fields.non_version_fields()
    final_report_fields = report_fields.order_fields()

    """Generate aggregation pipelines for use in later stages"""
    m = master.create_project(version_fields)
    d = diff.create_project(version_fields)
    f_f = fact_find.pipeline
    l = latest.pipeline
    f = final_report.create_project(version_fields)

    """List all collections in the source DB"""
    collections_source = list_collections(source_db)

    """Add strings IDs to all document for use in aggregation pipeline $lookup stage"""
    add_string_id_to_document(source_db, collections_source,
                              ['event', 'audit', 'chunks', 'excel', 'lookup', 'files'])

    """Retrieve other fields to index from report config"""
    required_indexes = get_config(cfg_file, 'SourceDB')['indexes']

    """Parse report config indexes and pass to add_index to create indexes"""
    add_index(source_db, parse_index_config(required_indexes))

    """Create the limited subset of factfind audit for use in main aggregation pipeline"""
    run_aggregate(fact_find_audit_collection, f_f, 'create')

    """Run aggregate against the person collection to generate the latest data"""
    run_aggregate(person_collection, l, 'create')

    # TODO: Test all code from REPL from this point onwards
    """Move created report collection from source_db to destination_db"""
    move_collection(db_name, db_name_destination, 'report.adviceJourneyLatest', 'adviceJourneyLatest')

    """Create master report collection if not exists, this should happen on 1st run of code in environment only"""
    collections_destination = list_collections(destination_db)

    if 'adviceJourneyMaster' not in collections_destination:
        run_aggregate(connection_collection_stage, m, 'create')

    """Create a cursor of all records in the latest data"""
    stage_cursor = connection_collection_stage.find()

    """Intialise bulk operation for adding missing records and fields and applying version diff"""
    bulk_new_master_records = connection_collection_master.initialize_unordered_bulk_op()
    bulk_add_master_records = connection_collection_master.initialize_unordered_bulk_op()
    bulk_master_diff = connection_collection_master.initialize_unordered_bulk_op()

    """Insert new records to the master collection"""
    dtc = DateTimes()
    insert_missing_master_records(stage_cursor, connection_collection_master, version_fields, non_version_fields,
                                  bulk_new_master_records,
                                  dtc.get_ms_time_utc())

    """Insert missing fields to the master collection"""
    master_cursor = connection_collection_master.find()
    insert_missing_master_fields(master_cursor, version_fields, non_version_fields, bulk_add_master_records)

    """Create a cursor to identify the version difference between the master and latest versions of data"""
    diff_cursor = run_aggregate(connection_collection_master, d, 'get')
    apply_diff_to_master(diff_cursor, bulk_master_diff, final_report_fields, version_fields, dtc.get_ms_time_utc())

    return run_aggregate(connection_collection_master, f, 'get')

    # except KeyError as main_error:
    #     logging.error('Failed to execute main function with error: %s', main_error)


if __name__ == '__main__':

    if sys.argv[1:2] is not None:
        import logging.config

        logging.config.fileConfig(sys.argv[1])

        main(sys.argv[1])
    else:
        print 'No config file passed in cmd line, please see help for more information'
