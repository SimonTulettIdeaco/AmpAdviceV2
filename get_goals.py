import xlsxwriter
from pymongo import MongoClient

from Sanbox.lookups import consults, ignore


def get_goals(cursor, out):
    for item in cursor:
        if item['consultationId'] in consults and item['consultationId'] not in ignore:
            goals = [
                [str(x.get('_id')),
                 item.get('consultationId'),
                 x.get('personId'),
                 item.get('snapshotKey'),
                 x.get('goalId'),
                 x.get('name'),
                 x.get('scope'),
                 x.get('priorityScore'),
                 x.get('tagline'),
                 x.get('progressStep'),
                 x.get('progressTrack'),
                 x.get('notes'),
                 x.get('reasonText'),
                 x.get('onHold'),
                 x.get('isCustomGoal')] for x in
                item.get('goals')]
            if len(goals) > 0:
                out.append(goals)
    return out


def get_objectives(cursor, out):
    if cursor.alive is False:
        cursor.rewind()

    for item in cursor:
        if item['consultationId'] in consults and item['consultationId'] not in ignore and item.get(
                'objectives') is not None:

            objectives = [
                [
                    x.get('goalObjectId'),
                    item['consultationId'],
                    x.get('personId'),
                    item['snapshotKey'],
                    x.get('template').get('text'),
                    ''.join(x.get('answers').get('OQ_PriorityScore')[0:1]),
                    ''.join(x.get('answers').get('OQ_TimeScore')[0:1]),
                    x.get('answers').get('OQ_PaymentsAmount'),
                    x.get('answers').get('OQ_AchievementAge'),
                    x.get('answers').get('OQ_NumYears'),
                ] for x in
                item.get('objectives')]
            if len(objectives) > 0:
                out.append(objectives)
    return out


class DictQuery(dict):
    def get(self, path, default=None):
        keys = path.split("/")
        val = None

        for key in keys:
            if val:
                if isinstance(val, list):
                    val = [v.get(key, default) if v else None for v in val]
                else:
                    val = val.get(key, default)
            else:
                val = dict.get(self, key, default)

            if not val:
                break

        return val


def get_strategies(cursor, out):
    # TODO: May need to extract person at every level not just top
    # TODO: May make sense to re write this as a recursive function instead

    for item in cursor:
        if item['consultationId'] in consults and item['consultationId'] not in ignore:
            print 'Breakpoint'

            strategies = [
                [
                    item['consultationId'],
                    DictQuery(x).get('strategyHeaders/statuses/personId'),
                    x.get('_id'),
                    x.get('name'),
                    DictQuery(x).get('strategyHeaders/_id'),
                    DictQuery(x).get('strategyHeaders/name'),
                    DictQuery(x).get('strategyHeaders/statuses/status'),
                    DictQuery(x).get('strategyHeaders/strategies/_id'),
                    DictQuery(x).get('strategyHeaders/strategies/name'),
                    DictQuery(x).get('strategyHeaders/strategies/explorerProgress'),
                    DictQuery(x).get('strategyHeaders/strategies/isSelectedForReview'),
                    DictQuery(x).get('strategyHeaders/strategies/relatedGoals/_id'),
                    DictQuery(x).get('strategyHeaders/strategies/statuses/status'),
                ] for x in
                item.get('modules')]
            if len(strategies) > 0:
                out.append(strategies)
    return out


def write_excel(header_row, data, xl_ws, fmt_header, fmt_font):
    # TODO: Convert formats to key word args
    """Takes a list of lists and inserts them into an excel spreadsheet

    Args:
        header_row: Headers for the excel worksheet
        data: list of lists to iterate over
        xl_ws: excel worksheet
        fmt_header: format to be applied to the header
        fmt_font: format to be applied to the data

    Returns:
        No return value"""
    col_pos = 0
    for h in header_row:
        xl_ws.write(0, col_pos, h, fmt_header)
        col_pos += 1

    row_pos = 1
    for li in data:
        for ll in li:
            col_pos = 0
            for col in ll:
                xl_ws.write(row_pos, col_pos, col, fmt_font)
                col_pos += 1
            row_pos += 1

    # Add filter to the header row
    xl_ws.autofilter(0, 0, 0, len(header_row))


def main():
    workbook = xlsxwriter.Workbook('C:\Users\Simon\Documents\Projects\AMP\AMP_Advice\Data\customerChoices.xlsx')
    goals_sheet = workbook.add_worksheet("customerGoals")
    objectives_sheet = workbook.add_worksheet("customerObjectives")
    strategies_sheet = workbook.add_worksheet("customerStrategies")
    bold = workbook.add_format({'bold': 1, 'font_size': 8})
    font = workbook.add_format({'font_size': 8})

    goals_header = ['GoalInstanceID', 'ConsultationId', 'PersonID', 'Snapshot', 'GoalID', 'GoalName', 'Scope',
                    'PriorityScore', 'GoalTagLine',
                    'ProgressStep',
                    'ProgressTrack', 'Notes', 'ReasonText', 'OnHold', 'isCustomGoal']
    objectives_header = ['GoalInstanceID', 'ConsultationId', 'PersonID', 'Snapshot', 'ObjectiveName', 'PriorityScore',
                         'TimeScore',
                         'Payments', 'AchievementAge', 'NumYears']

    strategies_header = ['ConsultationId', 'PersonId', 'ParentStrategyID', 'ParentStrategyName', 'L2StrategyID',
                         'L2StrategyName', 'L2Status', 'L3StrategyID', 'L3StrategyName', 'L3ExplorerProgress',
                         'L3IsSelectedForReview', 'L3RelatedGoals', 'L3Status']

    db = MongoClient().amp_blue_syd_scrubbed
    goals = db.goals
    strategies = db.strategies

    goals_cursor = goals.find()
    # strategies_cursor = strategies.find()

    goals_data = get_goals(goals_cursor, [])

    objectives_data = get_objectives(goals_cursor, [])

    # strategies_data = get_strategies(strategies_cursor, [])

    write_excel(goals_header, goals_data, goals_sheet, bold, font)

    write_excel(objectives_header, objectives_data, objectives_sheet, bold, font)

    # write_excel(strategies_header, strategies_data, strategies_sheet, bold, font)

    # Close the workbook
    workbook.close()


if __name__ == '__main__':
    main()
