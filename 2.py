fetchOrderProgressData

[
    {
        "ASNCount": 1,
        "FFIDCount": 1,
        "SNCount": 1,
        "WOCount": 1,
        "children": [
            {
                "ASNCount": 1,
                "SNCount": 1,
                "WOCount": 1,
                "children": [
                    {
                        "ASNCount": 1,
                        "SNCount": 1,
                        "children": [
                            {
                                "SNCount": 1,
                                "children": [
                                    {
                                        "expanded": true,
                                        "id": "CHVSN60159443416309056",
                                        "label": "SN Number",
                                        "status": "In Progress"
                                    }
                                ],
                                "expanded": true,
                                "id": "TMAV5112908146",
                                "label": "ASN Number",
                                "status": "In Progress"
                            }
                        ],
                        "expanded": true,
                        "id": "30000151655",
                        "label": "Work Order ID",
                        "status": "Completed"
                    }
                ],
                "expanded": true,
                "id": "3400310179",
                "label": "Fullfillment ID",
                "status": "In Progress"
            }
        ],
        "expanded": true,
        "id": "8400248146",
        "label": "Sales Order ID",
        "status": "In Progress"
    }
]

orderprogress/status

{
    "data": {
        "getSalesOrderHierarchy": {
            "salesOrders": [
                {
                    "OICIDs": [
                        {
                            "OICID": "9uCLw-vJEfCHM3OM8zllYw",
                            "fulfillmentIds": [
                                {
                                    "FulfillmentId": "3400310179",
                                    "fulfillmentOrders": [
                                        {
                                            "FOID": "42767201774563328",
                                            "status": "COMPLETED",
                                            "workOrders": [
                                                {
                                                    "ASNs": [
                                                        {
                                                            "ASNNumber": "TMAV5112908146",
                                                            "SNs": [
                                                                {
                                                                    "SNNumber": "CHVSN60159443416309056",
                                                                    "status": "COMPLETED"
                                                                }
                                                            ],
                                                            "status": "COMPLETED"
                                                        }
                                                    ],
                                                    "status": "COMPLETED",
                                                    "workOrderId": "30000151655"
                                                }
                                            ]
                                        }
                                    ],
                                    "status": "COMPLETED"
                                }
                            ],
                            "status": "NO_STATUS"
                        }
                    ],
                    "SONumber": "8400248146",
                    "status": "COMPLETED"
                }
            ]
        }
    },
    "logs": {
        "urls": [
            "https://keysphereservice-amer.uslge4b-r4-np.kob.dell.com/findby"
        ],
        "time": [
            "1.24s"
        ]
    }
}

