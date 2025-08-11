[
  {
    "data": {
      "getBySalesorderids": {
        "result": [
          {
            "asnNumbers": [],
            "fulfillment": [
              {
                "fulfillmentId": "537576",
                "fulfillmentStatus": "PP",
                "oicId": "95b8c514-070e-45ce-bef0-3e30773a9c36",
                "sourceSystemStatus": "PP"
              }
            ],
            "fulfillmentOrders": [
              {
                "foId": "7327243849264562177"
              }
            ],
            "salesOrder": {
              "buid": "10276",
              "region": "EMEA",
              "salesOrderId": "494070",
              "createDate": "2025-05-11 09:11:05.324333"
            },
            "workOrders": []
          }
        ]
      }
    }
  },
  {
    "data": {
      "getFulfillmentsById": [
        {
          "soHeaderRef": "7327243840485240832",
          "buid": 10276,
          "salesOrderId": "494070",
          "region": "EMEA",
          "sourceSystemId": "OPS",
          "fulfillments": [
            {
              "systemQty": 1,
              "shipByDate": "1900-01-01T00:00:00",
              "updateDate": "2025-05-11T09:11:05.930696",
              "shipCode": "Air",
              "mustArriveByDate": "1900-01-01T00:00:00",
              "mergeType": "ATO-INDIRECT",
              "manifestDate": "1900-01-01T00:00:00",
              "revisedDeliveryDate": "1900-01-01T00:00:00",
              "deliveryCity": null,
              "oicId": "95b8c514-070e-45ce-bef0-3e30773a9c36",
              "paymentTerm": "IMMEDIATE",
              "salesOrderLines": [
                {
                  "lob": "Inspiron 14 5440",
                  "facility": "APCX",
                  "siNumber": null,
                  "soLineNum": 1,
                  "specialinstructions": []
                }
              ],
              "sostatus": [
                {
                  "sourceSystemStsCode": "PP",
                  "statusDate": null,
                  "fulfillmentStsCode": "PP"
                }
              ],
              "address": [
                {
                  "taxRegstrnNum": null,
                  "addressLine1": "Dersden",
                  "postalCode": "01001",
                  "stateCode": null,
                  "cityCode": null,
                  "customerNum": null,
                  "customerNameExt": null,
                  "country": "DE",
                  "createDate": "2025-05-11T09:11:05.912142"
                },
                {
                  "taxRegstrnNum": null,
                  "addressLine1": "Dersden",
                  "postalCode": "01001",
                  "stateCode": null,
                  "cityCode": null,
                  "customerNum": null,
                  "customerNameExt": null,
                  "country": "DE",
                  "createDate": "2025-05-11T09:11:05.912142"
                }
              ]
            }
          ]
        }
      ],
      "getSoheaderBySoids": [
        {
          "buid": 10276,
          "ppDate": null,
          "orderDate": "2025-03-15T14:29:06",
          "salesOrderId": "494070",
          "agreementId": null,
          "totalPrice": 6894.62,
          "currency": "USD",
          "poNumber": "Po123",
          "dpid": "494070",
          "locationNum": "01",
          "rateUsdTransactional": 0.0,
          "updateDate": "2025-05-11T09:11:05.324333",
          "address": [
            {
              "companyName": "Iron Mountain Service GmbH - V2",
              "cityCode": null,
              "city": "Dersden",
              "firstName": "Michael",
              "lastName": null,
              "country": "DE",
              "stateCode": null,
              "addressLine1": "Dersden",
              "addressLine2": "Dersden",
              "postalCode": "01001",
              "contact": [
                {
                  "contactType": "SOLDTO"
                }
              ],
              "phone": [
                {
                  "phoneNumber": "1-12345678"
                }
              ]
            },
            {
              "companyName": "Iron Mountain Service GmbH - V2",
              "cityCode": null,
              "city": "Dersden",
              "firstName": "Nick",
              "lastName": "Miller",
              "country": "DE",
              "stateCode": null,
              "addressLine1": "Dersden",
              "addressLine2": "Dersden",
              "postalCode": "01001",
              "contact": [
                {
                  "contactType": "BILLING"
                }
              ],
              "phone": [
                {
                  "phoneNumber": "49-9876543201"
                }
              ]
            }
          ],
          "salesrep": [
            {
              "salesRepName": null
            }
          ]
        }
      ]
    }
  },
  {
    "data": {
      "getByFulfillmentids": {
        "result": [
          {
            "fulfillment": {
              "fulfillmentId": "537576",
              "oicId": "95b8c514-070e-45ce-bef0-3e30773a9c36",
              "fulfillmentStatus": "PP",
              "sourceSystemStatus": "PP"
            },
            "fulfillmentOrders": [
              {
                "foId": "7327243849264562177"
              }
            ],
            "workOrders": [],
            "salesOrder": {
              "salesOrderId": "494070",
              "buid": "10276",
              "region": "EMEA",
              "createDate": "2025-05-11 09:11:05.324333"
            },
            "asnNumbers": []
          }
        ]
      }
    }
  }
  ]
