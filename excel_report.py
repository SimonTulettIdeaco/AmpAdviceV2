# TODO: Test running the v1 1st to create a data set and then run v2 over the top
import numpy
import pandas as pd
import matplotlib.pyplot as plt

from get_data import main, GetReportFields, run_aggregate, convert_dates
from aggregationPipelines.report_fields import fields
from aggregationPipelines.get_final_report import create_project as cp
from pymongo import MongoClient
from datetime import datetime

db = MongoClient().amp_blue_syd_report
coll = db.adviceJourneyMaster

rf = GetReportFields(fields)

header = [x.get('alias') for x in rf.order_fields()]

date_calc_valid = rf.runtime_eligible_fields()

print date_calc_valid

results = []

results.append(header)

data = run_aggregate(coll, cp(rf.version_fields()), 'get')

for item in data:
    temp_list = []
    for i, d in enumerate(header):
        temp_list.insert(i, convert_dates(item.get(d)))
    results.append(temp_list)

# print results[0:10]

data_table = pd.DataFrame(results, columns=header)

print data_table[date_calc_valid].astype('M')
