"""Creates a mongo aggregation framework query to return the correct version of the fields, (latest or oldest)
 in the report"""
import logging
import pytz
import aggregationPipelines.r2_customers as r2

from datetime import datetime

logger = logging.getLogger(__name__)

# TODO: Think about how to handle the deletion of data that is populated into an audit collection
# TODO: Maybe Add PDF upload to DraftAdviceDocumentFinalised as well as word document

try:
    UTC = pytz.utc
    ZERO_DATE = datetime.fromtimestamp(0, pytz.utc)
    CURRENT_DATE = datetime.utcnow()
    CURRENT_DATE_UTC = datetime.now(pytz.utc)
    R1_DATE = UTC.localize(datetime.strptime('2016-08-22', '%Y-%m-%d'))

    UTC_DATE_DIFF = R1_DATE - ZERO_DATE
    CURRENT_DATE_DIFF = CURRENT_DATE_UTC - ZERO_DATE
    R1_MS = (UTC_DATE_DIFF.days * 86400000) + (UTC_DATE_DIFF.seconds * 1000) + (UTC_DATE_DIFF.microseconds / 1000)
    CURRENT_MS = (CURRENT_DATE_DIFF.days * 86400000) + (CURRENT_DATE_DIFF.seconds * 1000) + (
        CURRENT_DATE_DIFF.microseconds / 1000)

except AttributeError as date_error:
    logger.error('Failed to run date calculation login in get_latest_version pipeline with error : %s', date_error)
    raise SystemExit()

pipeline = ([
    {
        "$match":
            {
                "customerRole":
                    {
                        "$exists": True
                    },
                "activated": True
            }
    },
    {
        "$project":
            {
                "_id": 0,
                "PersonID": "$personIDstr",
                "latestConsultationId": "$latestConsultationId",
                "CustomerRole|ConciergePersonID": "$customerRole.conciergePersonId",
                "CustomerRole|PlannerPersonID": "$customerRole.plannerPersonId",
                "CustomerRole|PracticeID": "$customerRole.practice",
                "FirstName": "$firstName",
                "LastName": "$lastName",
                "ExistingCustomer": {"$ifNull": ["$existingCustomer", False]},
                "InitialClientWorkStarted":
                    {
                        "$max": ["$consents.PersonalInfo.consentDate", "$consents.ThirdParties.consentDate",
                                 "$consents.AccurateInfo.consentDate"]
                    }
            }
    },
    {
        "$lookup":
            {
                "from": "consultation",
                "localField": "PersonID",
                "foreignField": "clients.personId",
                "as": "consultation"
            }
    },
    {
        "$unwind":
            {
                "path": "$consultation",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": "$consultation.adviceReason",
                "InitialFeePackageChosen": "$consultation.adviceBuilder.feesSection.packageTypeInitial",
                "InitialFeePackageAmount": {
                    "$arrayElemAt": ["$consultation.adviceBuilder.feesSection.initialFees.dollarAmount", 0]},
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": "$consultation._id",
                "ConsultationID": "$consultation.consultationIDstr",
                "ProfileCreated": "$consultation.created",
                "DropOut":
                    {
                        "$cond":
                            {
                                "if":
                                    {
                                        "$gt": ["$consultation.dropout", None]
                                    },
                                "then": True,
                                "else": False
                            }
                    },
                "DropOutDate": "$consultation.dropout.date",
                "DropOutReason": "$consultation.dropout.reason",
                "HasPreviousConsultation":
                    {
                        "$cond":
                            {
                                "if":
                                    {
                                        "$gt": ["$consultation.previousConsultationIds._values", None]
                                    },
                                "then": True,
                                "else": False
                            }
                    },
                "LatestConsultation":
                    {
                        "$cond":
                            {
                                "if":
                                    {
                                        "$eq": ["$latestConsultationId", "$consultation.consultationIDstr"]
                                    },
                                "then": True,
                                "else": False
                            }
                    },
                "RegistrationComplete":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultation.journey",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$anyElementTrue":
                                                                            {
                                                                                "$map":
                                                                                    {
                                                                                        "input": ["Registered"],
                                                                                        "as": "compare",
                                                                                        "in":
                                                                                            {
                                                                                                "$eq": [
                                                                                                    "$$orig.stateId",
                                                                                                    "$$compare"]
                                                                                            }
                                                                                    }
                                                                            }
                                                                    }, "$$orig.whenEntered", False]
                                                            }
                                                    }
                                            },
                                        "as": "reg",
                                        "cond":
                                            {
                                                "$ne": ["$$reg", False]
                                            }
                                    }
                            }
                    },
                "ClientProfileAccepted":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultation.journey",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$anyElementTrue":
                                                                            {
                                                                                "$map":
                                                                                    {
                                                                                        "input": ["PlannerAccepted"],
                                                                                        "as": "compare",
                                                                                        "in":
                                                                                            {
                                                                                                "$eq": [
                                                                                                    "$$orig.stateId",
                                                                                                    "$$compare"]
                                                                                            }
                                                                                    }
                                                                            }
                                                                    }, "$$orig.whenEntered", False]
                                                            }
                                                    }
                                            },
                                        "as": "reg",
                                        "cond":
                                            {
                                                "$ne": ["$$reg", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$lookup":
            {
                "from": "fs.files",
                "localField": "ConsultationID",
                "foreignField": "metadata.consultationId",
                "as": "files"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": 1,
                "ConsultationID": 1,
                "ProfileCreated": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "CapitalPreferencesTool":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$files",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.metadata.displayName",
                                                                                "Capital Preferences Report"]
                                                                    }, "$$orig.metadata.createdTimeStamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    },
                "Conversation2MeetingAttended":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$files",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.metadata.displayName",
                                                                                "Favourite Scenario Screenshot"]
                                                                    }, "$$orig.metadata.createdTimeStamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    },
                "GoalsSummaryDocumentLastModification":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$files",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.metadata.displayName",
                                                                                "Goal Summary"]
                                                                    }, "$$orig.metadata.createdTimeStamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    },
                "StrategyPapersLastModification":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$files",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.metadata.displayName",
                                                                                "Advice exploration summary"]
                                                                    }, "$$orig.metadata.createdTimeStamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    },
                "StrategyPapersPrepared":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$files",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.metadata.displayName",
                                                                                "Advice exploration summary"]
                                                                    }, "$$orig.metadata.createdTimeStamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    },
                "DraftDocumentFinalisedLastModification":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$files",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.metadata.displayName",
                                                                                "SoA / RoA"]
                                                                    }, "$$orig.metadata.createdTimeStamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    },
                "FinalDocumentsLastModification":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$files",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.metadata.displayName",
                                                                                "Final SoA/RoA Presented"]
                                                                    }, "$$orig.metadata.createdTimeStamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$lookup":
            {
                "from": "goals",
                "localField": "ConsultationID",
                "foreignField": "consultationId",
                "as": "goals"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": 1,
                "ConsultationID": 1,
                "ProfileCreated": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "Conversation1MeetingAttended":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$goals",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.snapshotKey", "current"]
                                                                    }, "$$orig.whenCreated", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    },
                "GoalsSummaryDocumentPrepared":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$goals",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.snapshotKey",
                                                                                "afterGoalExplorer"]
                                                                    }, "$$orig.whenCreated", False]
                                                            }
                                                    }
                                            },
                                        "as": "gs",
                                        "cond":
                                            {
                                                "$ne": ["$$gs", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$lookup":
            {
                "from": "booking",
                "localField": "ConsultationID",
                "foreignField": "consultationId",
                "as": "booking"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": 1,
                "ConsultationID": 1,
                "ProfileCreated": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "Conversation1MeetingAttended": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "booking": "$booking",
                "ReviewMeetingScheduled": {
                    "$min":
                        {
                            "$filter":
                                {
                                    "input":
                                        {
                                            "$map":
                                                {
                                                    "input": "$booking",
                                                    "as": "b",
                                                    "in":
                                                        {
                                                            "$cond": [
                                                                {
                                                                    "$and": [
                                                                        {
                                                                            "$or": [
                                                                                {
                                                                                    "$eq": [
                                                                                        "$$b.cancelledDate",
                                                                                        0]
                                                                                },
                                                                                {
                                                                                    "$gt": [
                                                                                        "$$b.cancelledDate",
                                                                                        None]
                                                                                }]
                                                                        },
                                                                        {
                                                                            "$eq": ["$$b.type",
                                                                                    "reviewMeeting"]
                                                                        },
                                                                        {
                                                                            "$ne": ["$$b.status",
                                                                                    "cancelled"]
                                                                        }
                                                                    ]
                                                                }, "$$b.requestedTime", False]
                                                        }
                                                }},
                                    "as": "fm",
                                    "cond":
                                        {
                                            "$ne": ["$$fm", False]
                                        }
                                }
                        }
                },
                "IntroductoryMeetingScheduled": {
                    "$min":
                        {
                            "$filter":
                                {
                                    "input":
                                        {
                                            "$map":
                                                {
                                                    "input": "$booking",
                                                    "as": "b",
                                                    "in":
                                                        {
                                                            "$cond": [
                                                                {
                                                                    "$and": [
                                                                        {
                                                                            "$or": [
                                                                                {
                                                                                    "$eq": [
                                                                                        "$$b.cancelledDate",
                                                                                        0]
                                                                                },
                                                                                {
                                                                                    "$gt": [
                                                                                        "$$b.cancelledDate",
                                                                                        None]
                                                                                }]
                                                                        },
                                                                        {
                                                                            "$eq": ["$$b.type",
                                                                                    "introductoryMeeting"]
                                                                        },
                                                                        {
                                                                            "$ne": ["$$b.status",
                                                                                    "cancelled"]
                                                                        }
                                                                    ]
                                                                }, "$$b.requestedTime", False]
                                                        }
                                                }},
                                    "as": "fm",
                                    "cond":
                                        {
                                            "$ne": ["$$fm", False]
                                        }
                                }
                        }
                },
                "AdviceStrategyMeetingScheduled": {
                    "$min":
                        {
                            "$filter":
                                {
                                    "input":
                                        {
                                            "$map":
                                                {
                                                    "input": "$booking",
                                                    "as": "b",
                                                    "in":
                                                        {
                                                            "$cond": [
                                                                {
                                                                    "$and": [
                                                                        {
                                                                            "$or": [
                                                                                {
                                                                                    "$eq": [
                                                                                        "$$b.cancelledDate",
                                                                                        0]
                                                                                },
                                                                                {
                                                                                    "$gt": [
                                                                                        "$$b.cancelledDate",
                                                                                        None]
                                                                                }]
                                                                        },
                                                                        {
                                                                            "$eq": ["$$b.type",
                                                                                    "strategyMeeting"]
                                                                        },
                                                                        {
                                                                            "$ne": ["$$b.status",
                                                                                    "cancelled"]
                                                                        }
                                                                    ]
                                                                }, "$$b.requestedTime", False]
                                                        }
                                                }},
                                    "as": "fm",
                                    "cond":
                                        {
                                            "$ne": ["$$fm", False]
                                        }
                                }
                        }
                },
                "Conversation1MeetingCreated":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$let":
                                                    {
                                                        "vars":
                                                            {
                                                                "goalsDocPrepared": "$CapitalPreferencesTool",
                                                                "strategyPrepared": "$StrategyPapersPrepared",
                                                                "goalsMeetingAttended": "$Conversation1MeetingAttended",
                                                                "draftDocument": "$DraftDocumentFinalisedLastModification"
                                                            },
                                                        "in":
                                                            {
                                                                "$map":
                                                                    {
                                                                        "input": "$booking",
                                                                        "as": "b",
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$and": [
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            0]
                                                                                                    },
                                                                                                    {
                                                                                                        "$gt": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            None]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$eq": ["$$b.type",
                                                                                                        "firstMeeting"]
                                                                                            },
                                                                                            {
                                                                                                "$ne": ["$$b.status",
                                                                                                        "cancelled"]
                                                                                            }
                                                                                            ,
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$goalsDocPrepared",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$goalsDocPrepared"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$strategyPrepared",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$strategyPrepared"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$goalsMeetingAttended",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$goalsMeetingAttended"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$draftDocument",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$draftDocument"]
                                                                                                    }]
                                                                                            }]
                                                                                    }, "$$b.whenCreated", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "fm",
                                        "cond":
                                            {
                                                "$ne": ["$$fm", False]
                                            }
                                    }
                            }
                    },
                "Conversation1MeetingScheduled":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$let":
                                                    {
                                                        "vars":
                                                            {
                                                                "goalsDocPrepared": "$CapitalPreferencesTool",
                                                                "strategyPrepared": "$StrategyPapersPrepared",
                                                                "goalsMeetingAttended": "$Conversation1MeetingAttended",
                                                                "draftDocument": "$DraftDocumentFinalisedLastModification"
                                                            },
                                                        "in":
                                                            {
                                                                "$map":
                                                                    {
                                                                        "input": "$booking",
                                                                        "as": "b",
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$and": [
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            0]
                                                                                                    },
                                                                                                    {
                                                                                                        "$gt": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            None]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$eq": ["$$b.type",
                                                                                                        "firstMeeting"]
                                                                                            },
                                                                                            {
                                                                                                "$ne": ["$$b.status",
                                                                                                        "cancelled"]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$goalsDocPrepared",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$goalsDocPrepared"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$strategyPrepared",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$strategyPrepared"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$goalsMeetingAttended",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$goalsMeetingAttended"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$draftDocument",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$draftDocument"]
                                                                                                    }]
                                                                                            }]
                                                                                    }, "$$b.requestedTime", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "fm",
                                        "cond":
                                            {
                                                "$ne": ["$$fm", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": 1,
                "ConsultationID": 1,
                "ProfileCreated": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "Conversation1MeetingAttended": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "booking": 1,
                "Conversation2MeetingCreated":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$let":
                                                    {
                                                        "vars":
                                                            {
                                                                "strategyPrepared": "$StrategyPapersPrepared",
                                                                "goalsMeetingCreated": "$Conversation1MeetingCreated",
                                                                "draftDocument": "$DraftDocumentFinalisedLastModification",
                                                                "conversation2MeetingAttended": "$Conversation2MeetingAttended"
                                                            },
                                                        "in":
                                                            {
                                                                "$map":
                                                                    {
                                                                        "input": "$booking",
                                                                        "as": "b",
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$and": [
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            0]
                                                                                                    },
                                                                                                    {
                                                                                                        "$gt": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            None]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$eq": ["$$b.type",
                                                                                                        "scopeConfirmationMeeting"]
                                                                                            },
                                                                                            {
                                                                                                "$ne": ["$$b.status",
                                                                                                        "cancelled"]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$strategyPrepared",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$strategyPrepared"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$draftDocument",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$draftDocument"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$conversation2MeetingAttended",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$conversation2MeetingAttended"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$goalsMeetingCreated"]
                                                                                            }]
                                                                                    }, "$$b.whenCreated", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "fm",
                                        "cond":
                                            {
                                                "$ne": ["$$fm", False]
                                            }
                                    }
                            }
                    },
                "Conversation2MeetingScheduled":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$let":
                                                    {
                                                        "vars":
                                                            {
                                                                "strategyPrepared": "$StrategyPapersPrepared",
                                                                "goalsMeetingCreated": "$Conversation1MeetingCreated",
                                                                "draftDocument": "$DraftDocumentFinalisedLastModification",
                                                                "conversation2MeetingAttended": "$Conversation2MeetingAttended"
                                                            },
                                                        "in":
                                                            {
                                                                "$map":
                                                                    {
                                                                        "input": "$booking",
                                                                        "as": "b",
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$and": [
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            0]
                                                                                                    },
                                                                                                    {
                                                                                                        "$gt": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            None]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$eq": ["$$b.type",
                                                                                                        "scopeConfirmationMeeting"]
                                                                                            },
                                                                                            {
                                                                                                "$ne": ["$$b.status",
                                                                                                        "cancelled"]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$strategyPrepared",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$strategyPrepared"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$draftDocument",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$draftDocument"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$conversation2MeetingAttended",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$conversation2MeetingAttended"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$goalsMeetingCreated"]
                                                                                            }]
                                                                                    }, "$$b.requestedTime", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "fm",
                                        "cond":
                                            {
                                                "$ne": ["$$fm", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": 1,
                "ConsultationID": 1,
                "ProfileCreated": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "Conversation1MeetingAttended": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "booking": 1,
                "Conversation3MeetingCreated":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$let":
                                                    {
                                                        "vars":
                                                            {
                                                                "goalsMeetingCreated": "$Conversation1MeetingCreated",
                                                                "con2MeetingCreated": "$Conversation2MeetingCreated",
                                                                "finalDocument": "$FinalDocumentsLastModification",
                                                                "goalsMeetingAttended": "$Conversation1MeetingAttended",
                                                                "conversation2MeetingAttended": "$Conversation2MeetingAttended"
                                                            },
                                                        "in":
                                                            {
                                                                "$map":
                                                                    {
                                                                        "input": "$booking",
                                                                        "as": "b",
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$and": [
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            0]
                                                                                                    },
                                                                                                    {
                                                                                                        "$gt": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            None]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$eq": ["$$b.type",
                                                                                                        "secondMeeting"]
                                                                                            },
                                                                                            {
                                                                                                "$ne": ["$$b.status",
                                                                                                        "cancelled"]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$finalDocument",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$finalDocument"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$con2MeetingCreated"]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$goalsMeetingCreated"]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$goalsMeetingAttended"]
                                                                                            }
                                                                                            ,
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$conversation2MeetingAttended"]
                                                                                            }
                                                                                        ]
                                                                                    }, "$$b.whenCreated", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "fm",
                                        "cond":
                                            {
                                                "$ne": ["$$fm", False]
                                            }
                                    }
                            }
                    },
                "Conversation3MeetingScheduled":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$let":
                                                    {
                                                        "vars":
                                                            {
                                                                "goalsMeetingCreated": "$Conversation1MeetingCreated",
                                                                "con2MeetingCreated": "$Conversation2MeetingCreated",
                                                                "finalDocument": "$FinalDocumentsLastModification",
                                                                "goalsMeetingAttended": "$Conversation1MeetingAttended",
                                                                "conversation2MeetingAttended": "$Conversation2MeetingAttended"
                                                            },
                                                        "in":
                                                            {
                                                                "$map":
                                                                    {
                                                                        "input": "$booking",
                                                                        "as": "b",
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$and": [
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            0]
                                                                                                    },
                                                                                                    {
                                                                                                        "$gt": [
                                                                                                            "$$b.cancelledDate",
                                                                                                            None]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$eq": ["$$b.type",
                                                                                                        "secondMeeting"]
                                                                                            },
                                                                                            {
                                                                                                "$ne": ["$$b.status",
                                                                                                        "cancelled"]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$finalDocument",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$b.whenCreated",
                                                                                                            "$$finalDocument"]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$con2MeetingCreated"]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$goalsMeetingCreated"]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$goalsMeetingAttended"]
                                                                                            },
                                                                                            {
                                                                                                "$gt": [
                                                                                                    "$$b.whenCreated",
                                                                                                    "$$conversation2MeetingAttended"]
                                                                                            }]
                                                                                    }, "$$b.requestedTime", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "fm",
                                        "cond":
                                            {
                                                "$ne": ["$$fm", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$lookup":
            {
                "from": "scenarios",
                "localField": "ConsultationID",
                "foreignField": "consultation_id",
                "as": "scenario"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": 1,
                "ConsultationID": 1,
                "ProfileCreated": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "Conversation1MeetingAttended": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "Conversation2MeetingPrepared":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$let":
                                                    {
                                                        "vars":
                                                            {
                                                                "strategyPrepared": "$StrategyPapersPrepared",
                                                                "con2MeetingCreated": "$Conversation2MeetingCreated",
                                                                "draftDocument": "$DraftDocumentFinalisedLastModification"
                                                            },
                                                        "in":
                                                            {
                                                                "$map":
                                                                    {
                                                                        "input": "$scenario",
                                                                        "as": "s",
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$and": [
                                                                                            {
                                                                                                "$eq": [
                                                                                                    "$$s.scenario_id",
                                                                                                    "scenario_1"]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$strategyPrepared",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$s.created",
                                                                                                            {
                                                                                                                "$add": [
                                                                                                                    ZERO_DATE,
                                                                                                                    "$$strategyPrepared"]
                                                                                                            }]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$or": [
                                                                                                    {
                                                                                                        "$eq": [
                                                                                                            "$$draftDocument",
                                                                                                            None]
                                                                                                    },
                                                                                                    {
                                                                                                        "$lt": [
                                                                                                            "$$s.created",
                                                                                                            {
                                                                                                                "$add": [
                                                                                                                    ZERO_DATE,
                                                                                                                    "$$draftDocument"]
                                                                                                            }]
                                                                                                    }]
                                                                                            },
                                                                                            {
                                                                                                "$gt": ["$$s.created",
                                                                                                        {
                                                                                                            "$add": [
                                                                                                                ZERO_DATE,
                                                                                                                "$$con2MeetingCreated"]
                                                                                                        }]
                                                                                            }]
                                                                                    }, "$$s.created", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "fm",
                                        "cond":
                                            {
                                                "$ne": ["$$fm", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$lookup":
            {
                "from": "practiceDetails",
                "localField": "CustomerRole|PracticeID",
                "foreignField": "practiceDetailsIDstr",
                "as": "practiceDetails"
            }
    }
    ,
    {
        "$match": {
            "practiceDetails.businessName": {
                "$not": {"$in": ["The Archive", "R2TestPractice", "RCR Financial", "Alpha Financial Group Pty Ltd"]}}
        }
    }
    ,
    {
        "$project":
            {
                "PersonID": 1,
                "CustomerRole|ConciergePersonID": 1,
                "CustomerRole|PlannerPersonID": 1,
                "CustomerRole|PracticeID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "latestConsultationId": 1,
                "InitialClientWorkStarted": 1,
                "consultationOid": 1,
                "ConsultationID": 1,
                "ProfileCreated": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "Conversation1MeetingAttended": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation2MeetingPrepared": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "FinalDocumentsLastModification": 1,
                "PracticeName": "$practiceDetails.businessName",
                "PracticeType": "$practiceDetails.practiceType",
                "PracticeSite": "$practiceDetails.city",
                "PracticeLicensee": "$practiceDetails.licenseeLegalName",
                "PracticeState": "$practiceDetails.state"
            }
    },
    {
        "$unwind":
            {
                "path": "$PracticeName",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$PracticeType",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$PracticeSite",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$PracticeState",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$PracticeLicensee",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$lookup":
            {
                "from": "person",
                "localField": "CustomerRole|PlannerPersonID",
                "foreignField": "personIDstr",
                "as": "plannerPerson"
            }
    },
    {
        "$lookup":
            {
                "from": "person",
                "localField": "CustomerRole|ConciergePersonID",
                "foreignField": "personIDstr",
                "as": "conciergePerson"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "ConsultationID": 1,
                "consultationOid": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "PracticeName": 1,
                "PracticeType": 1,
                "PracticeSite": 1,
                "PracticeLicensee": 1,
                "PracticeState": 1,
                "AdviserFirstName": "$plannerPerson.firstName",
                "AdviserLastName": "$plannerPerson.lastName",
                "ConciergeFirstName": "$conciergePerson.firstName",
                "ConciergeLastName": "$conciergePerson.lastName",
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "ProfileCreated": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "InitialClientWorkStarted": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "Conversation1MeetingAttended": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation2MeetingPrepared": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1
            }
    },
    {
        "$unwind":
            {
                "path": "$AdviserFirstName",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$AdviserLastName",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$ConciergeFirstName",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$ConciergeLastName",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$lookup":
            {
                "from": "message",
                "localField": "ConsultationID",
                "foreignField": "about.consultationId",
                "as": "message"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "ConsultationID": 1,
                "consultationOid": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "PracticeName": 1,
                "PracticeType": 1,
                "PracticeSite": 1,
                "PracticeLicensee": 1,
                "PracticeState": 1,
                "AdviserFirstName": 1,
                "AdviserLastName": 1,
                "ConciergeFirstName": 1,
                "ConciergeLastName": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "ProfileCreated": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "InitialClientWorkStarted": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "Conversation1MeetingAttended": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "Conversation2MeetingPrepared": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "ClientProfileCompleted":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$message",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$eq": ["$$orig.about.reason",
                                                                                "homeworkAccepted"]
                                                                    }, "$$orig.createdTimestamp", False]
                                                            }
                                                    }
                                            },
                                        "as": "m",
                                        "cond":
                                            {
                                                "$ne": ["$$m", False]
                                            }
                                    }
                            }
                    },
                "ClientProfileReviewed":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$message",
                                                        "as": "orig",
                                                        "in":
                                                            {
                                                                "$cond": [
                                                                    {
                                                                        "$and": [
                                                                            {
                                                                                "$eq": ["$$orig.about.reason",
                                                                                        "homeworkAccepted"]
                                                                            },
                                                                            {
                                                                                "$eq": ["$$orig.read", True]
                                                                            }]
                                                                    },
                                                                    "$$orig.updatedTimestamp",
                                                                    False
                                                                ]
                                                            }
                                                    }
                                            },
                                        "as": "m",
                                        "cond":
                                            {
                                                "$ne": ["$$m", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$lookup":
            {
                "from": "consultation.audit",
                "localField": "consultationOid",
                "foreignField": "recordId",
                "as": "consultationAudit"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "ConsultationID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "PracticeName": 1,
                "PracticeType": 1,
                "PracticeSite": 1,
                "PracticeLicensee": 1,
                "PracticeState": 1,
                "AdviserFirstName": 1,
                "AdviserLastName": 1,
                "ConciergeFirstName": 1,
                "ConciergeLastName": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "ProfileCreated": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "ClientProfileCompleted": 1,
                "ClientProfileReviewed":
                    {
                        "$cond": [
                            {
                                "$or": [
                                    {
                                        "$gt": ["$ClientProfileReviewed", "$ClientProfileAccepted"]
                                    },
                                    {
                                        "$eq": ["$ClientProfileAccepted", None]
                                    }]
                            }, None, "$ClientProfileReviewed"]
                    },
                "InitialClientWorkStarted": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "Conversation1MeetingAttended": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "Conversation2MeetingPrepared": 1,
                "StrategyPapersLastModification": 1,
                "StrategyPapersPrepared": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "AdviceBuilderCompleted":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultationAudit",
                                                        "as": "level1",
                                                        "in":
                                                            {
                                                                "$let":
                                                                    {
                                                                        "vars":
                                                                            {
                                                                                "date": "$$level1.ts",
                                                                                "level2": "$$level1.data.__d"
                                                                            },
                                                                        "in":
                                                                            {
                                                                                "$filter":
                                                                                    {
                                                                                        "input":
                                                                                            {
                                                                                                "$map":
                                                                                                    {
                                                                                                        "input": "$$level2",
                                                                                                        "as": "arrayValue",
                                                                                                        "in":
                                                                                                            {
                                                                                                                "$cond": [
                                                                                                                    {
                                                                                                                        "$eq": [
                                                                                                                            "$$arrayValue",
                                                                                                                            "bpmsFolderId"]
                                                                                                                    },
                                                                                                                    "$$date",
                                                                                                                    False
                                                                                                                ]
                                                                                                            }
                                                                                                    }
                                                                                            },
                                                                                        "as": "av",
                                                                                        "cond":
                                                                                            {
                                                                                                "$ne": ["$$av", False]
                                                                                            }
                                                                                    }
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "p",
                                        "cond":
                                            {
                                                "$and": [
                                                    {
                                                        "$ne": ["$$p", []]
                                                    },
                                                    {
                                                        "$ne": ["$$p", None]
                                                    }]
                                            }
                                    }
                            }
                    },
                "AuthorityToProceed":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultationAudit",
                                                        "as": "audit",
                                                        "in":
                                                            {
                                                                "$let":
                                                                    {
                                                                        "vars":
                                                                            {
                                                                                "date": "$$audit.ts",
                                                                                "proceed": "$$audit.data.adviceBuilder.finalClientDocuments.authorityToProceed.attachmentIds"
                                                                            },
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$gt": ["$$proceed", 0]
                                                                                    }, "$$date", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f",
                                        "cond":
                                            {
                                                "$ne": ["$$f", False]
                                            }
                                    }
                            }
                    },
                "DraftAdviceWordDocumentFinalised":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultationAudit",
                                                        "as": "audit",
                                                        "in":
                                                            {
                                                                "$let":
                                                                    {
                                                                        "vars":
                                                                            {
                                                                                "date": "$$audit.ts",
                                                                                "proceedWord": "$$audit.data.adviceBuilder.paraplanning.soaRoa.wordDocument.attachmentIds"
                                                                            },
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$gt": ["$$proceedWord", 0]
                                                                                    }, "$$date", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f",
                                        "cond":
                                            {
                                                "$ne": ["$$f", False]
                                            }
                                    }
                            }
                    },
                "DraftAdviceDocumentFinalised":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultationAudit",
                                                        "as": "audit",
                                                        "in":
                                                            {
                                                                "$let":
                                                                    {
                                                                        "vars":
                                                                            {
                                                                                "date": "$$audit.ts",
                                                                                "proceed": "$$audit.data.adviceBuilder.paraplanning.soaRoa.wordDocument.attachmentIds"
                                                                            },
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$gt": ["$$proceed", 0]
                                                                                    }, "$$date", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f",
                                        "cond":
                                            {
                                                "$ne": ["$$f", False]
                                            }
                                    }
                            }
                    },
                "Conversation3MeetingAttended":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultationAudit",
                                                        "as": "audit",
                                                        "in":
                                                            {
                                                                "$let":
                                                                    {
                                                                        "vars":
                                                                            {
                                                                                "date": "$$audit.ts",
                                                                                "proceed": "$$audit.data.adviceBuilder.finalClientDocuments.soaRoaPresentedToClient.attachmentIds"
                                                                            },
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$gt": ["$$proceed", 0]
                                                                                    }, "$$date", False]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f",
                                        "cond":
                                            {
                                                "$ne": ["$$f", False]
                                            }
                                    }
                            }
                    },
                "ProceedToOnGoingAdviceFeesReady":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultationAudit",
                                                        "as": "audit",
                                                        "in":
                                                            {
                                                                "$let":
                                                                    {
                                                                        "vars":
                                                                            {
                                                                                "date": "$$audit.ts",
                                                                                "feesReady": "$$audit.data.adviceBuilder.sectionStatus.fees"
                                                                            },
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$gt": ["$$feesReady", 0]
                                                                                    }
                                                                                    , "$$date", False
                                                                                ]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f",
                                        "cond":
                                            {
                                                "$ne": ["$$f", False]
                                            }
                                    }
                            }
                    },
                "ProceedToOnGoingAdviceOngoingFees":
                    {
                        "$min":
                            {
                                "$filter":
                                    {
                                        "input":
                                            {
                                                "$map":
                                                    {
                                                        "input": "$consultationAudit",
                                                        "as": "audit",
                                                        "in":
                                                            {
                                                                "$let":
                                                                    {
                                                                        "vars":
                                                                            {
                                                                                "date": "$$audit.ts",
                                                                                "ongoingFees": "$$audit.data.adviceBuilder.ongoingFees"
                                                                            },
                                                                        "in":
                                                                            {
                                                                                "$cond": [
                                                                                    {
                                                                                        "$gt": ["$$ongoingFees", 0]
                                                                                    }, "$$date", False
                                                                                ]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f",
                                        "cond":
                                            {
                                                "$ne": ["$$f", False]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$unwind":
            {
                "path": "$AdviceBuilderCompleted",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$lookup":
            {
                "from": "factfind",
                "localField": "ConsultationID",
                "foreignField": "consultationId",
                "as": "factfind"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "ConsultationID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "PracticeName": 1,
                "PracticeType": 1,
                "PracticeSite": 1,
                "PracticeLicensee": 1,
                "PracticeState": 1,
                "AdviserFirstName": 1,
                "AdviserLastName": 1,
                "ConciergeFirstName": 1,
                "ConciergeLastName": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "ProfileCreated": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "ClientProfileCompleted": 1,
                "ClientProfileReviewed": 1,
                "InitialClientWorkStarted": 1,
                "StrategyPapersPrepared": 1,
                "DraftAdviceDocumentFinalised":
                    {
                        "$max": ["$DraftAdviceDocumentFinalised", "$DraftAdviceWordDocumentFinalised"]
                    },
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "Conversation1MeetingAttended": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "Conversation2MeetingPrepared": 1,
                "StrategyPapersLastModification": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "AdviceBuilderCompleted": 1,
                "Conversation3MeetingAttended": 1,
                "ProceedToOnGoingAdvice":
                    {
                        "$cond": [
                            {"$and":
                                [
                                    {"$gt":
                                        [
                                            "$ProceedToOnGoingAdviceFeesReady", 0]
                                    },
                                    {"$gt":
                                        [
                                            "$ProceedToOnGoingAdviceOngoingFees", 0
                                        ]
                                    }
                                ]
                            }
                            , True
                            , False
                        ]
                    },
                "AuthorityToProceed": 1,
                "FactFindID": "$factfind._id"
            }
    },
    {
        "$unwind":
            {
                "path": "$FactFindID",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$lookup":
            {
                "from": "report.factfind.audit",
                "localField": "FactFindID",
                "foreignField": "_id",
                "as": "factfindaudit"
            }
    },
    {
        "$project":
            {
                "PersonID": 1,
                "ConsultationID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "PracticeName": 1,
                "PracticeType": 1,
                "PracticeSite": 1,
                "PracticeLicensee": 1,
                "PracticeState": 1,
                "AdviserFirstName": 1,
                "AdviserLastName": 1,
                "ConciergeFirstName": 1,
                "ConciergeLastName": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "ProfileCreated": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "ClientProfileCompleted": 1,
                "ClientProfileReviewed": 1,
                "InitialClientWorkStarted": 1,
                "StrategyPapersPrepared": 1,
                "DraftAdviceDocumentFinalised": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "Conversation1MeetingAttended": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "Conversation2MeetingPrepared": 1,
                "StrategyPapersLastModification": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "AdviceBuilderCompleted": 1,
                "Conversation3MeetingAttended": 1,
                "AuthorityToProceed": 1,
                "ProceedToOnGoingAdvice": 1,
                "CoreProfile": "$factfindaudit.CoreProfileStarted",
                "DetailedClientWorkStarted": "$factfindaudit.DetailedClientWorkStarted"
            }
    },
    {
        "$unwind":
            {
                "path": "$CoreProfile",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$unwind":
            {
                "path": "$DetailedClientWorkStarted",
                "preserveNullAndEmptyArrays": True
            }
    },
    {
        "$project":
            {
                "_id": {"$concat": ["$PersonID", "$ConsultationID"]},
                "PersonID": 1,
                "ConsultationID": 1,
                "FirstName": 1,
                "LastName": 1,
                "ExistingCustomer": 1,
                "AdviserNotes": 1,
                "InitialFeePackageChosen": 1,
                "InitialFeePackageAmount": 1,
                "PracticeName": 1,
                "PracticeType": 1,
                "PracticeSite": 1,
                "PracticeLicensee": 1,
                "PracticeState": 1,
                "AdviserFirstName": 1,
                "AdviserLastName": 1,
                "ConciergeFirstName": 1,
                "ConciergeLastName": 1,
                "HasPreviousConsultation": 1,
                "DropOut": 1,
                "DropOutDate": 1,
                "DropOutReason": 1,
                "LatestConsultation": 1,
                "ProfileCreated": 1,
                "RegistrationComplete": 1,
                "ClientProfileAccepted": 1,
                "ClientProfileCompleted": 1,
                "ClientProfileReviewed": 1,
                "InitialClientWorkStarted": 1,
                "StrategyPapersPrepared": 1,
                "DraftAdviceDocumentFinalised": 1,
                "Conversation1MeetingCreated": 1,
                "Conversation1MeetingScheduled": 1,
                "ReviewMeetingScheduled": 1,
                "IntroductoryMeetingScheduled": 1,
                "AdviceStrategyMeetingScheduled": 1,
                "Conversation1MeetingAttended": 1,
                "GoalsSummaryDocumentPrepared": 1,
                "Conversation2MeetingCreated": 1,
                "Conversation2MeetingScheduled": 1,
                "Conversation3MeetingCreated": 1,
                "Conversation3MeetingScheduled": 1,
                "Conversation2MeetingPrepared": 1,
                "StrategyPapersLastModification": 1,
                "CapitalPreferencesTool": 1,
                "Conversation2MeetingAttended": 1,
                "GoalsSummaryDocumentLastModification": 1,
                "DraftDocumentFinalisedLastModification": 1,
                "FinalDocumentsLastModification": 1,
                "AdviceBuilderCompleted": 1,
                "Conversation3MeetingAttended": 1,
                "AuthorityToProceed": 1,
                "ProceedToOnGoingAdvice": 1,
                "CoreProfile": 1,
                "DetailedClientWorkStarted": 1,
                "R1Customer": {
                    "$cond": [{"$lt": [{"$ifNull": ["$ProfileCreated", "$RegistrationComplete"]},
                                       {
                                           "$cond":
                                               [
                                                   {"$eq": ["$PracticeName", 'AMP Advice Docklands']}
                                                   , r2.calculate_r2_ms('AMP Advice Docklands')
                                                   , {
                                                   "$cond":
                                                       [
                                                           {"$eq": ["$PracticeName", 'Mobbs Baker Wealth']}
                                                           , r2.calculate_r2_ms('Mobbs Baker Wealth')
                                                           , {
                                                           "$cond":
                                                               [
                                                                   {"$eq": ["$PracticeName", 'Templetons Financial']}
                                                                   , r2.calculate_r2_ms('Templetons Financial')
                                                                   , {
                                                                   "$cond":
                                                                       [
                                                                           {"$eq": ["$PracticeName",
                                                                                    'AMP Advice Newcastle']}
                                                                           , r2.calculate_r2_ms('AMP Advice Newcastle')
                                                                           , {
                                                                           "$cond":
                                                                               [
                                                                                   {"$eq": ["$PracticeName",
                                                                                            'AMP Advice Erina']}
                                                                                   , r2.calculate_r2_ms(
                                                                                   'AMP Advice Erina')
                                                                                   , {
                                                                                   "$cond":
                                                                                       [
                                                                                           {"$eq": ["$PracticeName",
                                                                                                    'AMP Advice Tweed Heads']}
                                                                                           , r2.calculate_r2_ms(
                                                                                           'AMP Advice Tweed Heads')
                                                                                           , {
                                                                                           "$cond":
                                                                                               [
                                                                                                   {"$eq": [
                                                                                                       "$PracticeName",
                                                                                                       'The Spark Store']}
                                                                                                   , r2.calculate_r2_ms(
                                                                                                   'The Spark Store')
                                                                                                   , False
                                                                                               ]
                                                                                       }
                                                                                       ]
                                                                               }
                                                                               ]
                                                                       }
                                                                       ]
                                                               }
                                                               ]
                                                       }
                                                       ]
                                               }
                                               ]
                                       }]}, True,
                              False]}
            }
    }, {"$out": "report.adviceJourneyLatest"}
]
)
