import json
import logging

logging.info('Starting data extraction process')

pipeline = (
    [
        {
            "$project":
                {
                    "_id": 0,
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
                    "LatestConsultation": 1,
                    "ExistingCustomer": 1,
                    "AdviserNotes": 1,
                    "Conversation1MeetingCreated": 1,
                    "Conversation2MeetingCreated": 1,
                    "Conversation3MeetingCreated": 1
                }
        },
        {
            "$project":
                {
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
                    "HasPreviousConsultation": 1,
                    "ExistingCustomer": 1,
                    "AdviserNotes": 1,
                    "InitialFeePackageChosen": 1,
                    "InitialFeePackageAmount": 1,
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
                    "DraftAdviceDocumentFinalised": 1,
                    "Conversation1MeetingFirstCreated": 1,
                    "Conversation1MeetingLastCreated": 1,
                    "Conversation1MeetingCreatedCount":
                        {
                            "$cond": [
                                {
                                    "$and": [
                                        {
                                            "$ne": ["$Conversation1MeetingFirstCreated", None]
                                        },
                                        {
                                            "$ne": ["$Conversation1MeetingLastCreated", None]
                                        }]
                                },
                                {
                                    "$min":
                                        {
                                            "$filter":
                                                {
                                                    "input":
                                                        {
                                                            "$slice": ["$Conversation1MeetingCreated.vid", -1]
                                                        },
                                                    "as": "x",
                                                    "cond":
                                                        {
                                                            "$ne": ["$xx", None]
                                                        }
                                                }
                                        }
                                }, None
                            ]
                        },
                    "Conversation1MeetingScheduled": 1,
                    "Conversation1MeetingAttended": 1,
                    "ReviewMeetingScheduled": 1,
                    "IntroductoryMeetingScheduled": 1,
                    "AdviceStrategyMeetingScheduled": 1,
                    "GoalsSummaryDocumentPrepared": 1,
                    "Conversation2MeetingFirstCreated": 1,
                    "Conversation2MeetingLastCreated": 1,
                    "Conversation2MeetingCreatedCount":
                        {
                            "$cond": [
                                {
                                    "$and": [
                                        {
                                            "$ne": ["$Conversation2MeetingFirstCreated", None]
                                        },
                                        {
                                            "$ne": ["$Conversation2MeetingLastCreated", None]
                                        }]
                                },
                                {
                                    "$min":
                                        {
                                            "$filter":
                                                {
                                                    "input":
                                                        {
                                                            "$slice": ["$Conversation2MeetingCreated.vid", -1]
                                                        },
                                                    "as": "x",
                                                    "cond":
                                                        {
                                                            "$ne": ["$xx", None]
                                                        }
                                                }
                                        }
                                }, None
                            ]
                        },
                    "Conversation2MeetingScheduled": 1,
                    "Conversation2MeetingPrepared": 1,
                    "StrategyPapersPrepared": 1,
                    "Conversation3MeetingFirstCreated": 1,
                    "Conversation3MeetingLastCreated": 1,
                    "Conversation3MeetingCreatedCount":
                        {
                            "$cond": [
                                {
                                    "$and": [
                                        {
                                            "$ne": ["$Conversation3MeetingFirstCreated", None]
                                        },
                                        {
                                            "$ne": ["$Conversation3MeetingLastCreated", None]
                                        }]
                                },
                                {
                                    "$min":
                                        {
                                            "$filter":
                                                {
                                                    "input":
                                                        {
                                                            "$slice": ["$Conversation3MeetingCreated.vid", -1]
                                                        },
                                                    "as": "x",
                                                    "cond":
                                                        {
                                                            "$ne": ["$xx", None]
                                                        }
                                                }
                                        }
                                }, None
                            ]
                        },
                    "Conversation3MeetingScheduled": 1,
                    "Conversation3MeetingAttended": 1,
                    "StrategyPapersLastModification": 1,
                    "CapitalPreferencesTool": 1,
                    "Conversation2MeetingAttended": 1,
                    "GoalsSummaryDocumentLastModification": 1,
                    "DraftDocumentFinalisedLastModification": 1,
                    "FinalDocumentsLastModification": 1,
                    "AdviceBuilderCompleted": 1,
                    "AuthorityToProceed": 1,
                    "ProceedToOnGoingAdvice": 1,
                    "CoreProfile": 1,
                    "DetailedClientWorkStarted": 1,
                    "R1Customer": 1
                }
        }
        ,
        {
            "$group":
                {
                    "_id":
                        {
                            "ConsultationID": "$ConsultationID",
                            "HasPreviousConsultation": "$HasPreviousConsultation",
                            "DropOut": "$DropOut",
                            "DropOutDate": "$DropOutDate",
                            "DropOutReason": "$DropOutReason",
                            "ProfileCreated": "$ProfileCreated",
                            "RegistrationComplete": "$RegistrationComplete",
                            "ClientProfileAccepted": "$ClientProfileAccepted",
                            "ClientProfileCompleted": "$ClientProfileCompleted",
                            "ClientProfileReviewed": "$ClientProfileReviewed",
                            "DraftAdviceDocumentFinalised": "$DraftAdviceDocumentFinalised",
                            "Conversation1MeetingFirstCreated": "$Conversation1MeetingFirstCreated",
                            "Conversation1MeetingLastCreated": "$Conversation1MeetingLastCreated",
                            "Conversation1MeetingCreatedCount": "$Conversation1MeetingCreatedCount",
                            "Conversation1MeetingScheduled": "$Conversation1MeetingScheduled",
                            "ReviewMeetingScheduled": "$ReviewMeetingScheduled",
                            "IntroductoryMeetingScheduled": "$IntroductoryMeetingScheduled",
                            "AdviceStrategyMeetingScheduled": "$AdviceStrategyMeetingScheduled",
                            "Conversation1MeetingAttended": "$Conversation1MeetingAttended",
                            "GoalsSummaryDocumentPrepared": "$GoalsSummaryDocumentPrepared",
                            "Conversation2MeetingFirstCreated": "$Conversation2MeetingFirstCreated",
                            "Conversation2MeetingLastCreated": "$Conversation2MeetingLastCreated",
                            "Conversation2MeetingCreatedCount": "$Conversation2MeetingCreatedCount",
                            "Conversation2MeetingScheduled": "$Conversation2MeetingScheduled",
                            "Conversation2MeetingPrepared": "$Conversation2MeetingPrepared",
                            "StrategyPapersPrepared": "$StrategyPapersPrepared",
                            "Conversation3MeetingFirstCreated": "$Conversation3MeetingFirstCreated",
                            "Conversation3MeetingLastCreated": "$Conversation3MeetingLastCreated",
                            "Conversation3MeetingCreatedCount": "$Conversation3MeetingCreatedCount",
                            "Conversation3MeetingScheduled": "$Conversation3MeetingScheduled",
                            "Conversation3MeetingAttended": "$Conversation3MeetingAttended",
                            "StrategyPapersLastModification": "$StrategyPapersLastModification",
                            "CapitalPreferencesTool": "$CapitalPreferencesTool",
                            "Conversation2MeetingAttended": "$Conversation2MeetingAttended",
                            "GoalsSummaryDocumentLastModification": "$GoalsSummaryDocumentLastModification",
                            "DraftDocumentFinalisedLastModification": "$DraftDocumentFinalisedLastModification",
                            "FinalDocumentsLastModification": "$FinalDocumentsLastModification",
                            "AdviceBuilderCompleted": "$AdviceBuilderCompleted",
                            "AuthorityToProceed": "$AuthorityToProceed",
                            "ProceedToOnGoingAdvice": "$ProceedToOnGoingAdvice",
                            "CoreProfile": "$CoreProfile",
                            "DetailedClientWorkStarted": "$DetailedClientWorkStarted",
                            "R1Customer": "$R1Customer",
                            "AdviserNotes": "$AdviserNotes",
                            "InitialFeePackageChosen": "$InitialFeePackageChosen",
                            "InitialFeePackageAmount": "$InitialFeePackageAmount",
                        }
                    , "People":
                    {
                        "$push":
                            {
                                "FirstName": "$FirstName",
                                "LastName": "$LastName",
                                "LatestConsultation": "$LatestConsultation",
                                "ExistingCustomer": "$ExistingCustomer"
                            }
                    },
                    "PracticeName": {"$max": "$PracticeName"},
                    "PracticeType": {"$max": "$PracticeType"},
                    "PracticeSite": {"$max": "$PracticeSite"},
                    "PracticeLicensee": {"$max": "$PracticeLicensee"},
                    "PracticeState": {"$max": "$PracticeState"},
                    "AdviserFirstName": {"$max": "$AdviserFirstName"},
                    "AdviserLastName": {"$max": "$AdviserLastName"},
                    "ConciergeFirstName": {"$max": "$ConciergeFirstName"},
                    "ConciergeLastName": {"$max": "$ConciergeLastName"},
                    "InitialClientWorkStarted": {"$max": "$InitialClientWorkStarted"},
                }
        }
        ,
        {
            "$project":
                {
                    "_id": 0,
                    "FirstNameOne": {"$arrayElemAt": ["$People.FirstName", 0]},
                    "FirstNameTwo": {"$arrayElemAt": ["$People.FirstName", 1]},
                    "LastNameOne": {"$arrayElemAt": ["$People.LastName", 0]},
                    "LastNameTwo": {"$arrayElemAt": ["$People.LastName", 1]},
                    "LatestConsultationOne": {"$arrayElemAt": ["$People.LatestConsultation", 0]},
                    "LatestConsultationTwo": {"$arrayElemAt": ["$People.LatestConsultation", 1]},
                    "ExistingCustomerOne": {"$arrayElemAt": ["$People.ExistingCustomer", 0]},
                    "ExistingCustomerTwo": {"$arrayElemAt": ["$People.ExistingCustomer", 1]},
                    "TotalCustomers": {"$size": "$People"},
                    "PracticeName": 1,
                    "PracticeType": 1,
                    "PracticeSite": 1,
                    "PracticeLicensee": 1,
                    "PracticeState": 1,
                    "AdviserFirstName": 1,
                    "AdviserLastName": 1,
                    "ConciergeFirstName": 1,
                    "ConciergeLastName": 1,
                    "InitialClientWorkStarted": 1,
                    "ConsultationID": "$_id.ConsultationID",
                    "AdviserNotes": "$_id.AdviserNotes",
                    "InitialFeePackageChosen": "$_id.InitialFeePackageChosen",
                    "InitialFeePackageAmount": "$_id.InitialFeePackageAmount",
                    "HasPreviousConsultation": "$_id.HasPreviousConsultation",
                    "DropOut": "$_id.DropOut",
                    "DropOutDate": "$_id.DropOutDate",
                    "DropOutReason": "$_id.DropOutReason",
                    "ProfileCreated": "$_id.ProfileCreated",
                    "RegistrationComplete": "$_id.RegistrationComplete",
                    "ClientProfileAccepted": "$_id.ClientProfileAccepted",
                    "ClientProfileCompleted": "$_id.ClientProfileCompleted",
                    "ClientProfileReviewed": "$_id.ClientProfileReviewed",
                    "DraftAdviceDocumentFinalised": "$_id.DraftAdviceDocumentFinalised",
                    "Conversation1MeetingFirstCreated": "$_id.Conversation1MeetingFirstCreated",
                    "Conversation1MeetingLastCreated": "$_id.Conversation1MeetingLastCreated",
                    "Conversation1MeetingCreatedCount": "$_id.Conversation1MeetingCreatedCount",
                    "Conversation1MeetingScheduled": "$_id.Conversation1MeetingScheduled",
                    "ReviewMeetingScheduled": "$_id.ReviewMeetingScheduled",
                    "IntroductoryMeetingScheduled": "$_id.IntroductoryMeetingScheduled",
                    "AdviceStrategyMeetingScheduled": "$_id.AdviceStrategyMeetingScheduled",
                    "Conversation1MeetingAttended": "$_id.Conversation1MeetingAttended",
                    "GoalsSummaryDocumentPrepared": "$_id.GoalsSummaryDocumentPrepared",
                    "Conversation2MeetingFirstCreated": "$_id.Conversation2MeetingFirstCreated",
                    "Conversation2MeetingLastCreated": "$_id.Conversation2MeetingLastCreated",
                    "Conversation2MeetingCreatedCount": "$_id.Conversation2MeetingCreatedCount",
                    "Conversation2MeetingScheduled": "$_id.Conversation2MeetingScheduled",
                    "Conversation2MeetingPrepared": "$_id.Conversation2MeetingPrepared",
                    "StrategyPapersPrepared": "$_id.StrategyPapersPrepared",
                    "Conversation3MeetingFirstCreated": "$_id.Conversation3MeetingFirstCreated",
                    "Conversation3MeetingLastCreated": "$_id.Conversation3MeetingLastCreated",
                    "Conversation3MeetingCreatedCount": "$_id.Conversation3MeetingCreatedCount",
                    "Conversation3MeetingScheduled": "$_id.Conversation3MeetingScheduled",
                    "Conversation3MeetingAttended": "$_id.Conversation3MeetingAttended",
                    "StrategyPapersLastModification": "$_id.StrategyPapersLastModification",
                    "CapitalPreferencesTool": "$_id.CapitalPreferencesTool",
                    "Conversation2MeetingAttended": "$_id.Conversation2MeetingAttended",
                    "GoalsSummaryDocumentLastModification": "$_id.GoalsSummaryDocumentLastModification",
                    "DraftDocumentFinalisedLastModification": "$_id.DraftDocumentFinalisedLastModification",
                    "FinalDocumentsLastModification": "$_id.FinalDocumentsLastModification",
                    "AdviceBuilderCompleted": "$_id.AdviceBuilderCompleted",
                    "AuthorityToProceed": "$_id.AuthorityToProceed",
                    "ProceedToOnGoingAdvice": "$_id.ProceedToOnGoingAdvice",
                    "CoreProfile": "$_id.CoreProfile",
                    "DetailedClientWorkStarted": "$_id.DetailedClientWorkStarted",
                    "R1Customer": "$_id.R1Customer"

                }
        }
    ]
)

vFieldsLatest = {
    "xxxA":
        {
            "$min":
                {
                    "$filter":
                        {
                            "input":
                                {
                                    "$slice": ["$xxxN.data", -1]
                                },
                            "as": "x",
                            "cond":
                                {
                                    "$ne": ["$xx", None]
                                }
                        }
                }
        },
}
vFieldsFirst = {
    "xxxA":
        {
            "$min":
                {
                    "$filter":
                        {
                            "input":
                                {
                                    "$slice": ["$xxxN.data", 1]
                                },
                            "as": "x",
                            "cond":
                                {
                                    "$ne": ["$xx", None]
                                }
                        }
                }
        },
}


# Create pipeline
def create_project(vf):
    try:
        for item in vf:
            if item.get('latestVersion') == 1:
                vfl = json.loads(
                    json.dumps(vFieldsLatest).replace('xxxN', item.get('pipeline')).replace('xxxA', item.get('alias')))
                pipeline[0]["$project"].update(vfl)
            elif item.get('latestVersion') == 0:
                vfo = json.loads(
                    json.dumps(vFieldsFirst).replace('xxxN', item.get('pipeline')).replace('xxxA', item.get('alias')))
                pipeline[0]["$project"].update(vfo)
        return pipeline
    except AttributeError as pipe_err:
        logging.error('Unable to create pipeline for version diff : %s', pipe_err)
        raise SystemExit()
