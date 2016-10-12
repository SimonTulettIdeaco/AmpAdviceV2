"""Creates a mongo aggregation framework query to create the master collection
which will hold the version history of changes to a report field"""
import json
import pytz
import logging

from datetime import datetime

logger = logging.getLogger(__name__)

# Define UTC dates
zeroDate = datetime.fromtimestamp(0, pytz.utc)
currentDate = datetime.now(pytz.utc)

# Calculate millisecond date
utcDDiff = currentDate - zeroDate
ms = (utcDDiff.days * 86400000) + (utcDDiff.seconds * 1000) + (utcDDiff.microseconds / 1000)

# Pipeline static
pipeline = [
    {
        "$project":
            {
                "_id": 1,
                "PersonID": 1,
                "ConsultationID": 1,
                "FirstName": 1,
                "LastName": 1,
                "PracticeName": 1,
                "PracticeType": 1,
                "PracticeSite": 1,
                "PracticeLicensee": 1,
                "PracticeState": 1,
                "AdviserFirstName": 1,
                "AdviserLastName": 1,
                "ConciergeFirstName": 1,
                "ConciergeLastName": 1,
                "AdviserNotes": 1,
                "InitialFeePackageAmount": 1,
                "InitialFeePackageChosen": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "R1Customer": 1,
                "HasPreviousConsultation": 1,
                "ProceedToOnGoingAdvice": 1,

            }
    },
    {
        "$out": "adviceJourneyMaster"
    }
]

# Pipeline dynamic
vFields = {
    "xxx":
        {
            "$cond": [
                {
                    "$or": [
                        {
                            "$eq": [
                                {
                                    "$ifNull": ["$xxx", -1]
                                }, -1]
                        },
                        {
                            "$lt": ["$xxx", 0]
                        }]
                },
                [],
                [
                    {
                        "vid": {"$literal": 1},
                        "data": "$xxx",
                        "ts": {"$literal": ms}
                    }]
            ]
        },
}


# Create Pipeline
def create_project(vf):
    # TODO: Handle the duplication of pipeline fields causing unnecessary iteration
    """"Dynamically generates a mongo aggregation framework query using the field list provided
    Args:
        vf: List of versioned fields
    Returns:
        Mongo aggregation framework query for use by pymongo
    """
    try:
        for item in vf:
            _, li, _ = item.values()
            dff = json.loads(json.dumps(vFields).replace('xxx', li))
            for d in pipeline:
                if "$project" in d.keys():
                    pipeline[0]["$project"].update(dff)
        return pipeline
    except AttributeError as pipe_err:
        logger.error('Unable to create pipeline for version master : %s', pipe_err)
        raise SystemExit()
