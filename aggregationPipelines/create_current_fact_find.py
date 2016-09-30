"""Creates a mongo aggregation framework query that creates a new collection in the source database to house a
subset of the fact find audit collection due to it's size"""

pipeline = ([
    {
        "$match":
            {
                "data.sectionRoot._sections._sections.percentComplete":
                    {
                        "$gt": 0
                    }
            }
    },
    {
        "$project":
            {
                "recordId": "$recordId",
                "date": "$ts",
                "data.sectionRoot._sections._sections._id": 1,
                "data.sectionRoot._sections._sections.percentComplete": 1
            }
    },
    {
        "$project":
            {
                "recordId": "$recordId",
                "data": "$data",
                "CoreProfile":
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
                                                                "date": "$date",
                                                                "coreWork": "$data.sectionRoot"
                                                            },
                                                        "in":
                                                            {
                                                                "$filter":
                                                                    {
                                                                        "input":
                                                                            {
                                                                                "$map":
                                                                                    {
                                                                                        "input": "$$coreWork._sections",
                                                                                        "as": "lvl2",
                                                                                        "in":
                                                                                            {
                                                                                                "$filter":
                                                                                                    {
                                                                                                        "input":
                                                                                                            {
                                                                                                                "$map":
                                                                                                                    {
                                                                                                                        "input": "$$lvl2._sections",
                                                                                                                        "as": "lvl3",
                                                                                                                        "in":
                                                                                                                            {
                                                                                                                                "$cond": [
                                                                                                                                    {
                                                                                                                                        "$and": [
                                                                                                                                            {
                                                                                                                                                "$eq": [
                                                                                                                                                    "$$lvl3._id",
                                                                                                                                                    "PD"]
                                                                                                                                            },
                                                                                                                                            {
                                                                                                                                                "$gt": [
                                                                                                                                                    "$$lvl3.percentComplete",
                                                                                                                                                    0]
                                                                                                                                            }
                                                                                                                                        ]
                                                                                                                                    },
                                                                                                                                    "$$date",
                                                                                                                                    False]
                                                                                                                            }
                                                                                                                    }
                                                                                                            },
                                                                                                        "as": "f1",
                                                                                                        "cond":
                                                                                                            {
                                                                                                                "$and": [
                                                                                                                    {
                                                                                                                        "$ne": [
                                                                                                                            "$$f1",
                                                                                                                            None]
                                                                                                                    },
                                                                                                                    {
                                                                                                                        "$ne": [
                                                                                                                            "$$f1",
                                                                                                                            []]
                                                                                                                    },
                                                                                                                    {
                                                                                                                        "$ne": [
                                                                                                                            "$$f1",
                                                                                                                            False]
                                                                                                                    }]
                                                                                                            }
                                                                                                    }
                                                                                            }
                                                                                    }
                                                                            },
                                                                        "as": "f2",
                                                                        "cond":
                                                                            {
                                                                                "$and": [
                                                                                    {
                                                                                        "$ne": ["$$f2", None]
                                                                                    },
                                                                                    {
                                                                                        "$ne": ["$$f2", []]
                                                                                    }]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f3",
                                        "cond":
                                            {
                                                "$and": [
                                                    {
                                                        "$ne": ["$$f3", None]
                                                    },
                                                    {
                                                        "$ne": ["$$f3", []]
                                                    }]
                                            }
                                    }
                            }
                    },
                "DetailedClientWork":
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
                                                                "date": "$date",
                                                                "coreWork": "$data.sectionRoot"
                                                            },
                                                        "in":
                                                            {
                                                                "$filter":
                                                                    {
                                                                        "input":
                                                                            {
                                                                                "$map":
                                                                                    {
                                                                                        "input": "$$coreWork._sections",
                                                                                        "as": "lvl2",
                                                                                        "in":
                                                                                            {
                                                                                                "$filter":
                                                                                                    {
                                                                                                        "input":
                                                                                                            {
                                                                                                                "$map":
                                                                                                                    {
                                                                                                                        "input": "$$lvl2._sections",
                                                                                                                        "as": "lvl3",
                                                                                                                        "in":
                                                                                                                            {
                                                                                                                                "$cond": [
                                                                                                                                    {
                                                                                                                                        "$and": [
                                                                                                                                            {
                                                                                                                                                "$ne": [
                                                                                                                                                    "$$lvl3._id",
                                                                                                                                                    "PD"]
                                                                                                                                            },
                                                                                                                                            {
                                                                                                                                                "$gt": [
                                                                                                                                                    "$$lvl3.percentComplete",
                                                                                                                                                    0]
                                                                                                                                            }
                                                                                                                                        ]
                                                                                                                                    },
                                                                                                                                    "$$date",
                                                                                                                                    False]
                                                                                                                            }
                                                                                                                    }
                                                                                                            },
                                                                                                        "as": "f1",
                                                                                                        "cond":
                                                                                                            {
                                                                                                                "$and": [
                                                                                                                    {
                                                                                                                        "$ne": [
                                                                                                                            "$$f1",
                                                                                                                            None]
                                                                                                                    },
                                                                                                                    {
                                                                                                                        "$ne": [
                                                                                                                            "$$f1",
                                                                                                                            []]
                                                                                                                    },
                                                                                                                    {
                                                                                                                        "$ne": [
                                                                                                                            "$$f1",
                                                                                                                            False]
                                                                                                                    }]
                                                                                                            }
                                                                                                    }
                                                                                            }
                                                                                    }
                                                                            },
                                                                        "as": "f2",
                                                                        "cond":
                                                                            {
                                                                                "$and": [
                                                                                    {
                                                                                        "$ne": ["$$f2", None]
                                                                                    },
                                                                                    {
                                                                                        "$ne": ["$$f2", []]
                                                                                    }]
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            },
                                        "as": "f3",
                                        "cond":
                                            {
                                                "$and": [
                                                    {
                                                        "$ne": ["$$f3", None]
                                                    },
                                                    {
                                                        "$ne": ["$$f3", []]
                                                    }]
                                            }
                                    }
                            }
                    }
            }
    },
    {
        "$match":
            {
                "$or": [
                    {
                        "CoreProfile":
                            {
                                "$ne": None
                            }
                    },
                    {
                        "DetailedClientWork":
                            {
                                "$ne": None
                            }
                    }]
            }
    },
    {
        "$project":
            {
                "recordId": "$recordId",
                "CoreProfileStarted":
                    {
                        "$min": ["$CoreProfile"]
                    },
                "DetailedClientWorkStarted":
                    {
                        "$min": ["$DetailedClientWork"]
                    }
            }
    },
    {
        "$group":
            {
                "_id": "$recordId",
                "CoreProfileStarted":
                    {
                        "$min": "$CoreProfileStarted"
                    },
                "DetailedClientWorkStarted":
                    {
                        "$min": "$DetailedClientWorkStarted"
                    }
            }
    },
    {
        "$out": "report.factfind.audit"
    }
]
)
