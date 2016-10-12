import copy
from datetime import datetime

import bson
import xlsxwriter
from pymongo import MongoClient

from Sanbox.lookups import consults, ignore
from get_journey import convert_dates

WORKBOOK = xlsxwriter.Workbook('C:\Users\Simon\Documents\Projects\AMP\AMP_Advice\Data\strategies.xlsx')
SHEET = WORKBOOK.add_worksheet("strategies")
BOLD = WORKBOOK.add_format({'bold': 1, 'font_size': 8})
FONT = WORKBOOK.add_format({'font_size': 8})
DATE = WORKBOOK.add_format({'font_size': 8, 'num_format': 'dd/mm/yyyy hh:mm:ss'})
#
# db = MongoClient().amp_blue_syd_scrubbed
# coll = db.goals
#
# goals_cursor = coll.find()
#
# master_list = []
# header = ['ConsultationId', 'PersonID', 'Snapshot', 'GoalID', 'GoalName', 'Scope', 'PriorityScore', 'GoalTagLine',
#           'ProgressStep',
#           'ProgressTrack', 'Notes', 'ReasonText', 'OnHold', 'isCustomGoal']
#
#
# def get_goals(cursor):
#     for item in cursor:
#         if item['consultationId'] in consults and item['consultationId'] not in ignore:
#             goals = [
#                 [item['consultationId'], x.get('personId'), item['snapshotKey'], x.get('goalId'), x.get('name'),
#                  x.get('scope'),
#                  x.get('priorityScore'),
#                  x.get('tagline'),
#                  x.get('progressStep'), x.get('progressTrack'),
#                  x.get('notes'), x.get('reasonText'), x.get('onHold'), x.get('isCustomGoal')] for x in
#                 item['goals']]
#             if len(goals) > 0:
#                 master_list.append(goals)
#     return master_list
#
#
# def write_excel(header_row, data):
#     col_pos = 0
#     for h in header_row:
#         SHEET.write(0, col_pos, h, BOLD)
#         col_pos += 1
#
#     row_pos = 1
#     for li in data:
#         for ll in li:
#             col_pos = 0
#             for col in ll:
#                 SHEET.write(row_pos, col_pos, col, FONT)
#                 col_pos += 1
#             row_pos += 1
#
#     # Add filter to the header row
#     SHEET.autofilter(0, 0, 0, 13)
#
#     # Close the workbook
#     WORKBOOK.close()
columns_exclude = [
    '_class'
    , 'modules_id'
    , '_id'
    , 'strategyHeaders_trashWarning'
    , 'strategies_soaTrashWarning'
    , 'strategies_trashWarning'
    , 'strategies_conditionExpression'
    , 'strategyHeaders_description'
    , 'strategies_description'
    , 'strategyHeaders_id'
]

columns_filter = ['relatedGoals_id', 'statuses_status']

db = MongoClient().amp_blue_syd_scrubbed

coll = db.strategies

"""My dic recurse generator function"""

# def dic_recurse(data, fields, counter, source_field):
#     if isinstance(data, dict):
#         counter += 1
#         for k, v in data.items():
#             if isinstance(v, list):
#                 for field_data in v:
#                     for list_field in dic_recurse(field_data, fields, counter, source_field):
#                         yield list_field
#             elif isinstance(v, dict):
#                 for dic_field in dic_recurse(v, fields, counter, source_field):
#                     yield dic_field
#             elif k in fields and isinstance(v, list) is False and isinstance(v, dict) is False:
#                 yield counter, {"{0}_L{1}".format(k, counter): v}
#     elif isinstance(data, list):
#         counter += 1
#         for list_item in data:
#             for li2 in dic_recurse(list_item, fields, counter, ''):
#                 yield li2


"""Recurse function found online slightly more succinct"""


def dict_generator(indict, unique_id, display_path, path_delimiter, field_path=None, membership_path=None):
    field_path = field_path[:] if field_path else []
    membership_path = membership_path[:] if membership_path else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                for d in dict_generator(value, unique_id, display_path, path_delimiter, field_path + [key],
                                        membership_path + [str(indict.get(locals()['unique_id'], 'Root'))] + [key]):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    for d in dict_generator(v, unique_id, display_path, path_delimiter, field_path + [key],
                                            membership_path + [str(indict.get(locals()['unique_id'], 'Root'))] + [key]):
                        yield d
            else:
                if display_path == 'Membership' and key not in locals()['unique_id']:
                    if len(membership_path) == 0:
                        yield (path_delimiter.join([unicode('Root')]), {key: value})
                    else:
                        yield (path_delimiter.join(membership_path[:-1]),
                               {membership_path[-1] + "_" + key.replace('_', ''): value})

                elif display_path != 'Membership' and key not in locals()['unique_id']:
                    if len(field_path) == 0:
                        yield (path_delimiter.join([unicode('Root')]), {key: value})
                    else:
                        yield (
                            path_delimiter.join(field_path[:-1]),
                            {field_path[-1] + "_" + key.replace('_', ''): value})
    else:
        yield indict


strategy_cursor = coll.find()

master_header_list = []
for item in strategy_cursor:
    if item['consultationId'] in consults and item['consultationId'] not in ignore:
        gen = dict_generator(item, 'name', 'Membership', '-->')
        for f in gen:
            if 'question' not in f[0]:
                master_header_list.append(f)

header = sorted(set([h for hl in [x.keys() for i in master_header_list for x in i if isinstance(x, dict)] for h in hl]))

strategy_cursor.rewind()

final_list = []
for item2 in strategy_cursor:
    if item2['consultationId'] in consults and item2['consultationId'] not in ignore:
        row_list = []
        gen = dict_generator(item2, 'name', 'Membership', '-->')
        for f in gen:
            if 'question' not in f[0]:
                row_list.append(f)
        final_list.append(row_list)

strategies = sorted(set([x[0] for i in final_list for x in i if not isinstance(x, dict)]),
                    key=lambda s: (s, len(s)))

unique_strategies = [[x] for x in strategies]

strategy_cursor.rewind()

report_list = []
for item3 in strategy_cursor:
    if item3['consultationId'] in consults and item3['consultationId'] not in ignore:
        final_list = []
        strategy_map_list = copy.deepcopy(unique_strategies)
        row_list = []
        gen = dict_generator(item3, 'name', 'Membership', '-->')
        for f in gen:
            if 'question' not in f[0]:
                row_list.append(f)
        final_list.append(row_list)
        for sub_list in final_list:
            for entry in sub_list:
                for strategy in unique_strategies:
                    if entry[0] == strategy[0] and strategy[0] != 'Root':
                        ix = [i for i, x in enumerate(strategy_map_list) if x[0] == entry[0]]
                        strategy_map_list[ix[0]].append(entry[1])

        for row in strategy_map_list:

            dupes_header_check = [x for y in row for x in y if not isinstance(y, unicode)]
            duplicate_columns = [col for col in header if dupes_header_check.count(col) > 1]

            sorted_rows = [row[0]] + sorted([x for x in row if isinstance(x, dict)], key=lambda s: s.keys())

            counter = 0
            current_column = ''

            final_results = {}
            final_results.update({sorted_rows[0]: []})

            for item in sorted_rows:
                if isinstance(item, dict) and "".join(item.keys()) in duplicate_columns and current_column == "".join(
                        item.keys()) and "".join(item.keys()) not in columns_exclude:
                    counter += 1
                    final_results[row[0]].append({"".join(item.keys()) + "_" + unicode(counter): item.values()[0]})
                    current_column = "".join(item.keys())
                elif isinstance(item, dict) and "".join(item.keys()) not in columns_exclude:
                    counter = 0
                    final_results[row[0]].append({"".join(item.keys()): item.values()[0]})
                    current_column = "".join(item.keys())
            final_results[row[0]].append({'consultationId': item3['consultationId']})
            final_results[row[0]].append({'whenCreated': item3['whenCreated']})
            final_results[row[0]].append({'whenModified': item3['whenModified']})

            report_list.append(final_results)


def report_sort(filter_value):
    try:
        int(filter_value.split("_")[-1])
        return filter_value.split("_")[0], int(filter_value.split("_")[-1])
    except:
        return filter_value.split("_")[0], 0

final_report_header = sorted(set(
    [a for b in [z.keys() for q in [y for x in [new_item.values() for new_item in report_list] for y in x] for z in q]
     for a in
     b]), key=report_sort)

final_report_header.insert(0, u'Strategy')

output = [list(final_report_header)]
for item in report_list:
    report_dic = {}
    for k, v in item.items():
        report_row = ["-->".join([x for i, x in enumerate(k.split("-->")[2:]) if i % 2 == 0])]
        report_dic = reduce(lambda r, d: r.update(d) or r, v, {})
        report_row.extend([convert_dates(report_dic.get(h)) for h in final_report_header[1:]])
        output.append(report_row)

filter_indexes = [i for i, x in enumerate(output[0]) if x in columns_filter]

filtered_output = []
for fr in output[1:]:
    for i, x in enumerate(fr):
        if i in filter_indexes and x is not None:
            filtered_output.append(fr)

final_output = [output[0]] + filtered_output

header_count = 0
row_count = 0
for item in final_output:
    if header_count == 0:
        col_pos = 0
        for c in item:
            SHEET.write(row_count, col_pos, c, BOLD)
            col_pos += 1
        header_count += 1
        row_count += 1
    else:
        col_pos = 0
        for c in item:
            if isinstance(c, bson.objectid.ObjectId):
                SHEET.write(row_count, col_pos, str(c), FONT)
                col_pos += 1
            elif isinstance(c, datetime):
                SHEET.write(row_count, col_pos, str(c), DATE)
                col_pos += 1
            else:
                SHEET.write(row_count, col_pos, c, FONT)
                col_pos += 1
        row_count += 1

# Add filter to the header row
SHEET.autofilter(0, 0, 0, 25)

# Close the workbook
WORKBOOK.close()
