import pytz

from datetime import datetime

UTC = pytz.utc
ZERO_DATE = datetime.fromtimestamp(0, pytz.utc)
CURRENT_DATE = datetime.utcnow()

R2Dates = [
    {'PracticeName': 'AMP Advice Docklands', 'Date': '2016-01-01', 'MS_Time': 0},
    {'PracticeName': 'Mobbs Baker Wealth', 'Date': '2016-08-31', 'MS_Time': 0},
    {'PracticeName': 'Templetons Financial', 'Date': '2016-08-31', 'MS_Time': 0},
    {'PracticeName': 'AMP Advice Newcastle', 'Date': '2016-01-01', 'MS_Time': 0},
    {'PracticeName': 'AMP Advice Erina', 'Date': '2016-01-01', 'MS_Time': 0},
    {'PracticeName': 'AMP Advice Tweed Heads', 'Date': '2016-01-01', 'MS_Time': 0},
    {'PracticeName': 'The Spark Store', 'Date': '2016-08-22', 'MS_Time': 0}
]


def calculate_r2_ms(practice_name):
    practice_date = [d['Date'] for d in R2Dates if
                     d['PracticeName'] == practice_name]
    local_date = UTC.localize(datetime.strptime(''.join(practice_date), '%Y-%m-%d'))
    date_diff = local_date - ZERO_DATE
    r1_ms = (date_diff.days * 86400000) + (date_diff.seconds * 1000) + (date_diff.microseconds / 1000)
    return r1_ms

