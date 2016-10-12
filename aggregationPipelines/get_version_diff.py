"""Creates a mongo aggregation framework query which identifies the version difference between the
Master collection and the latest data"""
import json
import logging

logger = logging.getLogger(__name__)

# TODO: Convert the diff on static fields to dynamically generated JSON
# TODO: Convert non versioned fields to dynamically generated $project as well
# TODO: Need to handle removal of non versioned fields, these are filtered out and not updated.
# Pipeline static
pipeline = (
    [
        {
            "$lookup":
                {
                    "from": "adviceJourneyLatest",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "newVersion"
                }
        },
        {
            "$unwind":
                {
                    "path": "$newVersion",
                    "preserveNullAndEmptyArrays": True
                }
        },
        {
            "$project":
                {
                    "_id": 1,
                    "PersonID": 1,
                    "ConsultationID": 1,
                    "FirstName": "$newVersion.FirstName",
                    "LastName": "$newVersion.LastName",
                    "LatestConsultation": "$newVersion.LatestConsultation",
                    "ExistingCustomer": "$newVersion.ExistingCustomer",
                    "AdviserNotes": "$newVersion.AdviserNotes",
                    "PracticeName": "$newVersion.PracticeName",
                    "PracticeType": "$newVersion.PracticeType",
                    "PracticeSite": "$newVersion.PracticeSite",
                    "PracticeLicensee": "$newVersion.PracticeLicensee",
                    "PracticeState": "$newVersion.PracticeState",
                    "AdviserFirstName": "$newVersion.AdviserFirstName",
                    "AdviserLastName": "$newVersion.AdviserLastName",
                    "ConciergeFirstName": "$newVersion.ConciergeFirstName",
                    "ConciergeLastName": "$newVersion.ConciergeLastName",
                    "InitialFeePackageAmount": "$newVersion.InitialFeePackageAmount",
                    "InitialFeePackageChosen": "$newVersion.InitialFeePackageChosen",
                    "DropOut": "$newVersion.DropOut",
                    "DropOutDate": "$newVersion.DropOutDate",
                    "DropOutReason": "$newVersion.DropOutReason",
                    "R1Customer": "$newVersion.R1Customer",
                    "HasPreviousConsultation": "$newVersion.HasPreviousConsultation",
                    "ProceedToOnGoingAdvice": "$newVersion.ProceedToOnGoingAdvice",
                    "NonVersionedChanged":
                        {
                            "$or":
                                [
                                    {"$and": [{"$gt": ["$newVersion.FirstName", 0]},
                                              {"$ne": ['$newVersion.FirstName', '$FirstName']}]},
                                    {"$and": [{"$gt": ["$newVersion.LastName", 0]},
                                              {"$ne": ['$newVersion.LastName', '$LastName']}]},
                                    {"$and": [{"$gt": ["$newVersion.LatestConsultation", 0]},
                                              {"$ne": ['$newVersion.LatestConsultation', '$LatestConsultation']}]},
                                    {"$and": [{"$gt": ["$newVersion.AdviserNotes", 0]},
                                              {"$ne": ['$newVersion.AdviserNotes', '$AdviserNotes']}]},
                                    {"$and": [{"$gt": ["$newVersion.ExistingCustomer", 0]},
                                              {"$ne": ['$newVersion.ExistingCustomer', '$ExistingCustomer']}]},
                                    {"$and": [{"$gt": ["$newVersion.PracticeName", 0]},
                                              {"$ne": ['$newVersion.PracticeName', '$PracticeName']}]},
                                    {"$and": [{"$gt": ["$newVersion.PracticeType", 0]},
                                              {"$ne": ['$newVersion.PracticeType', '$PracticeType']}]},
                                    {"$and": [{"$gt": ["$newVersion.PracticeLicensee", 0]},
                                              {"$ne": ['$newVersion.PracticeLicensee', '$PracticeLicensee']}]},
                                    {"$and": [{"$gt": ["$newVersion.PracticeState", 0]},
                                              {"$ne": ['$newVersion.PracticeState', '$PracticeState']}]},
                                    {"$and": [{"$gt": ["$newVersion.AdviserFirstName", 0]},
                                              {"$ne": ['$newVersion.AdviserFirstName', '$AdviserFirstName']}]},
                                    {"$and": [{"$gt": ["$newVersion.AdviserLastName", 0]},
                                              {"$ne": ['$newVersion.AdviserLastName', '$AdviserLastName']}]},
                                    {"$and": [{"$gt": ["$newVersion.ConciergeFirstName", 0]},
                                              {"$ne": ['$newVersion.ConciergeFirstName', '$ConciergeFirstName']}]},
                                    {"$and": [{"$gt": ["$newVersion.ConciergeLastName", 0]},
                                              {"$ne": ['$newVersion.ConciergeLastName', '$ConciergeLastName']}]},
                                    {"$and": [{"$gt": ["$newVersion.InitialFeePackageAmount", 0]},
                                              {"$ne": ['$newVersion.InitialFeePackageAmount',
                                                       '$InitialFeePackageAmount']}]},
                                    {"$and": [{"$gt": ["$newVersion.InitialFeePackageChosen", 0]},
                                              {"$ne": ['$newVersion.InitialFeePackageChosen',
                                                       '$InitialFeePackageChosen']}]},
                                    {"$and": [{"$gt": ["$newVersion.DropOut", 0]},
                                              {"$ne": ['$newVersion.DropOut', '$DropOut']}]},
                                    {"$and": [{"$gt": ["$newVersion.DropOutDate", 0]},
                                              {"$ne": ['$newVersion.DropOutDate', '$DropOutDate']}]},
                                    {"$and": [{"$gt": ["$newVersion.DropOutReason", 0]},
                                              {"$ne": ['$newVersion.DropOutReason', '$DropOutReason']}]},
                                    {"$and": [{"$gt": ["$newVersion.R1Customer", 0]},
                                              {"$ne": ['$newVersion.R1Customer', '$R1Customer']}]},
                                    {"$and": [{"$gt": ["$newVersion.HasPreviousConsultation", 0]},
                                              {"$ne": ['$newVersion.HasPreviousConsultation',
                                                       '$HasPreviousConsultation']}]},
                                    {"$and": [{"$gt": ["$newVersion.ProceedToOnGoingAdvice", 0]},
                                              {"$ne": ['$newVersion.ProceedToOnGoingAdvice',
                                                       '$ProceedToOnGoingAdvice']}]},

                                ]
                        },
                }
        }
        ,
        {
            "$match":
                {
                    "$or": [
                        {"NonVersionedChanged": {"$eq": True}},
                    ]
                }
        }
    ])

# Pipeline dynamic
vFields = {
    "xxx":
        {
            "$cond": [
                {
                    "$or": [
                        {
                            "$and": [
                                {
                                    "$eq": [
                                        {
                                            "$size": "$xxx"
                                        }, 0]
                                },
                                {
                                    "$eq": ["$newVersion.xxx", None]
                                }]
                        },
                        {
                            "$and": [
                                {
                                    "$eq": [
                                        {
                                            "$size": "$xxx"
                                        }, 0]
                                },
                                {
                                    "$lt": ["$newVersion.xxx", 0]
                                }]
                        },
                        {
                            "$and": [
                                {
                                    "$gt": [
                                        {
                                            "$size": "$xxx"
                                        }, 0]
                                },
                                {
                                    "$eq": ["$newVersion.xxx",
                                            {
                                                "$min":
                                                    {
                                                        "$slice": ["$xxx.data", -1]
                                                    }
                                            }]
                                }]
                        }]
                }, None, [
                    {
                        "vid":
                            {
                                "$ifNull": [
                                    {
                                        "$add": [
                                            {
                                                "$min":
                                                    {
                                                        "$slice": ["$xxx.vid", -1]
                                                    }
                                            }, 1]
                                    }, 1]
                            },
                        "data": {"$ifNull": ["$newVersion.xxx", None]}
                    }]]
        },
}
mFields = {
              "xxx":
                  {
                      "$ne": None
                  }
          },


# Create pipeline
def create_project(vf):
    """"Dynamically generates a mongo aggregation framework query using the field list provided
        Args:
            vf: List of versioned fields
        Returns:
            Mongo aggregation framework query for use by pymongo
        """
    try:
        for item in vf:
            _, li, _ = item.values()
            vfg = json.loads(json.dumps(vFields).replace('xxx', li))
            dfm = json.loads(json.dumps(mFields).replace('xxx', li))
            for d in pipeline:
                if "$project" in d.keys():
                    pipeline[2]["$project"].update(vfg)
                if "$match" in d.keys():
                    pipeline[3]["$match"]["$or"].append(dfm[0])
        return pipeline
    except AttributeError as pipe_err:
        logger.error('Unable to create pipeline for version diff : %s', pipe_err)
        raise SystemExit()
