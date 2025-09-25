def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    # print(json.dumps(result_map.get("sales_orders_summary"),indent=2))
    # print(json.dumps(result_map.get("graphql_details"),indent=2))
    # exit()
    try:
        def extract_sales_order(data):
            if not data or not isinstance(data, dict):
                return None

            soids_data = data.get("getSalesOrderBySoids")
            if soids_data:
                sales_orders = soids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders

            ffids_data = data.get("getSalesOrderByFfids")
            if ffids_data:
                sales_orders = ffids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders

            return None

        flat_list = []
        ValidCount = []

        # sales_wo_details contains the mapping of SalesOrder -> WOs
        sales_wo_details = result_map.get("sales_orders_summary", [])
        graphql_details = result_map.get("graphql_details", [])

        # Build a mapping for quick lookup of WO IDs by Sales Order ID
        so_wo_map = {so['salesOrderId']: so.get('workOrderIds', []) for so in sales_wo_details}

        for item in graphql_details:
            if not isinstance(item, dict):
                continue

            data = item.get("data", {})
            if not data:
                continue

            sales_orders = extract_sales_order(data)
            raw_workorders = data.get("getWorkOrderByWoIds", [])

            if not sales_orders:
                continue

            for so in sales_orders:
                sales_order_id = safe_get(so, ['salesOrderId'])
                if region and region.upper() != safe_get(so, ['region'], "").upper():
                    continue

                if filtersValue:
                    ValidCount.append(sales_order_id)

                fulfillments = safe_get(so, ['fulfillments']) or []
                # print(len(fulfillments))
                # print(json.dumps(fulfillments,indent=2))
                # exit()
                if isinstance(fulfillments, dict):
                    fulfillments = [fulfillments]

                shipping_addr = pick_address_by_type(so, "SHIPPING")
                billing_addr = pick_address_by_type(so, "BILLING")
                shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING") if fulfillments else None
                shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

                lob_list = list(filter(
                    lambda lob: lob and lob.strip() != "",
                    map(lambda line: safe_get(line, ['lob']), safe_get(fulfillments, [0,'salesOrderLines']) or [])
                ))
                lob = ", ".join(lob_list)

                facility_list = list(filter(
                    lambda f: f and f.strip() != "",
                    map(lambda line: safe_get(line, ['facility']), safe_get(fulfillments, [0,'salesOrderLines']) or [])
                ))
                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f))

                def get_status_date(code):
                    status_code = safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(fulfillments, [0, 'soStatus', 0, 'statusDate']))
                    return ""

                # Base row for Sales Order (WO fields empty by default)
                row = {
                    "Fulfillment ID": safe_get(fulfillments, [0, 'fulfillmentId']),
                    "BUID": safe_get(so, ['buid']),
                    "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                    "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "LOB": lob,
                    "Sales Order ID": sales_order_id,
                    "Agreement ID": safe_get(so, ['agreementId']),
                    "Amount": safe_get(so, ['totalPrice']),
                    "Currency Code": safe_get(so, ['currency']),
                    "Customer Po Number": safe_get(so, ['poNumber']),
                    "Delivery City": safe_get(fulfillments, [0, 'deliveryCity']),
                    "DOMS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "Dp ID": safe_get(so, ['dpid']),
                    "Fulfillment Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                    "Merge Type": safe_get(fulfillments, [0, 'mergeType']),
                    "InstallInstruction2": get_install_instruction2_id(so),
                    "PP Date": get_status_date("PP"),
                    "IP Date": get_status_date("IP"),
                    "MN Date": get_status_date("MN"),
                    "SC Date": get_status_date("SC"),
                    "Location Number": safe_get(so, ['locationNum']),
                    "OFS Status Code": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "OFS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                    "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "ShippingContactName": shipping_contact_name,
                    "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                    "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                    "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShipToPhone": (listify(shipping_phone.get("phone", []))[0].get("phoneNumber", "")
                                    if shipping_phone and listify(shipping_phone.get("phone", [])) else ""),
                    "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                    "Order Age": safe_get(so, ['orderDate']),
                    "Order Amount usd": safe_get(so, ['rateUsdTransactional']),
                    "Rate Usd Transactional": safe_get(so, ['rateUsdTransactional']),
                    "Sales Rep Name": safe_get(so, ['salesrep', 0, 'salesRepName']),
                    "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Source System Status": safe_get(fulfillments, [0, 'soStatus', 0,'sourceSystemStsCode']),
                    "Tie Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'soLineNum']),
                    "Si Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'siNumber']),
                    "Req Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                    "Reassigned IP Date": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "Payment Term Code": safe_get(fulfillments, [0, 'paymentTerm']),
                    "Region Code": safe_get(so, ['region']),
                    "FO ID": safe_get(fulfillments, [0, 'fulfillmentOrder', 0, 'foId']),
                    "System Qty": safe_get(fulfillments, [0, 'systemQty']),
                    "Ship By Date": safe_get(fulfillments, [0, 'shipByDate']),
                    "Facility": facility,
                    "Tax Regstrn Num": safe_get(fulfillments, [0, 'address', 0, 'taxRegstrnNum']),
                    "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
                    "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
                    "Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                    "Must Arrive By Date": dateFormation(safe_get(fulfillments, [0, 'mustArriveByDate'])),
                    "Manifest Date": dateFormation(safe_get(fulfillments, [0, 'manifestDate'])),
                    "Revised Delivery Date": dateFormation(safe_get(fulfillments, [0, 'revisedDeliveryDate'])),
                    "Source System ID": safe_get(so, ['sourceSystemId']),
                    "OIC ID": safe_get(fulfillments, [0, 'oicId']),
                    "Order Date": dateFormation(safe_get(so, ['orderDate'])),
                    "Order Type": dateFormation(safe_get(so, ['orderType'])),
                    "WO_ID": "",
                    "Dell Blanket PO Num": "",
                    "Ship To Facility": "",
                    "Is Last Leg": "",
                    "Ship From MCID": "",
                    "WO OTM Enabled": "",
                    "WO Ship Mode": "",
                    "Is Multipack": "",
                    "Has Software": "",
                    "Make WO Ack Date": "",
                    "MCID Value": ""
                }

                
                wo_ids = so_wo_map.get(sales_order_id, [])
                if wo_ids:
                    for WO_ID in wo_ids:
                        wo_obj = next((wo for wo in raw_workorders if wo.get("woId") == WO_ID), {})
                        wo_row = {
                            "WO_ID": WO_ID,
                            "Dell Blanket PO Num": safe_get(wo_obj, ['dellBlanketPoNum']),
                            "Ship To Facility": safe_get(wo_obj, ['shipToFacility']),
                            "Is Last Leg": 'Y' if safe_get(wo_obj, ['shipToFacility']) else 'N',
                            "Ship From MCID": safe_get(wo_obj, ['vendorSiteId']),
                            "WO OTM Enabled": safe_get(wo_obj, ['isOtmEnabled']),
                            "WO Ship Mode": safe_get(wo_obj, ['shipMode']),
                            "Is Multipack": safe_get(wo_obj, ['woLines', 0, 'ismultipack']),
                            "Has Software": any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in safe_get(wo_obj, ['woLines']) or []),
                            "Make WO Ack Date": next(
                                (dateFormation(status.get("statusDate"))
                                 for status in wo_obj.get("woStatusList", [])
                                 if str(status.get("channelStatusCode")) == "3000" and wo_obj.get("woType") == "MAKE"),
                                ""
                            ),
                            "MCID Value": (
                                safe_get(wo_obj, ['woShipInstr', 0, "mergeFacility"]) or
                                safe_get(wo_obj, ['woShipInstr', 0, "carrierHubCode"])
                            )
                        }
                        flat_list.append({**row, **wo_row})
                else:
                    flat_list.append(row)

        count_valid = len(ValidCount)
        if not flat_list:
            return {"error": "No Data Found"}

        if format_type == "export":
            data = [{"Count ": count_valid}, flat_list] if filtersValue else flat_list
            ValidCount.clear()
            return data

        elif format_type == "grid":
            desired_order = list(flat_list[0].keys())
            rows = []
            for item in flat_list:
                row = {"columns": [{"value": item.get(k, "")} for k in desired_order]}
                rows.append(row)
            table_grid_output = tablestructural(rows, region) if rows else []
            if filtersValue:
                table_grid_output["Count"] = count_valid
            ValidCount.clear()
            return table_grid_output

        return flat_list

    except Exception as e:
        return {"error": str(e)}

The above is  my code 

sales_wo_details = result_map.get("sales_orders_summary", [])
response of  sales_wo_details:
[
  {
    "salesOrderId": "8040070632",
    "region": "APJ",
    "fullfillments": [
      "3000070327",
      "3000070326"
    ],
    "foIds": [
      "7355860096558333953",
      "7355928803984830465"
    ],
    "workOrderIds": [
      "1888314027",
      "1122192942",
      "1127194521"
    ]
  }
]

right now i had getting 3 response is corret but fullfillment and foid data is coming first data only i need proper data 

raw data 
[
  {
    "data": {
      "getSalesOrderBySoids": {
        "salesOrders": [
          {
            "agreementId": "",
            "totalPrice": "11353.88",
            "buid": "10036",
            "currency": "AUD",
            "poNumber": "",
            "dpid": "8040070632",
            "locationNum": "01",
            "orderDate": "2025-07-29 06:59:04",
            "rateUsdTransactional": "0",
            "orderCreateDate": "2025-07-29 06:59:04",
            "sourceSystemId": "DSP",
            "salesOrderId": "8040070632",
            "region": "APJ",
            "orderType": "Standard Order",
            "address": [
              {
                "companyName": "",
                "cityCode": "",
                "city": "",
                "firstName": "",
                "lastName": "",
                "fullName": "",
                "country": "",
                "stateCode": "",
                "addressLine1": "",
                "addressLine2": "",
                "postalCode": "",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "INSTALLAT"
                  }
                ]
              },
              {
                "companyName": "Qantas Airways Limited",
                "cityCode": "",
                "city": "Mascot",
                "firstName": "Vinay",
                "lastName": "Kumar",
                "fullName": "Vinay Kumar",
                "country": "AU",
                "stateCode": "NSW",
                "addressLine1": "10 Bourke Road",
                "addressLine2": "",
                "postalCode": "2020",
                "customerNum": "P10013040501",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "END_USER"
                  }
                ]
              },
              {
                "companyName": "",
                "cityCode": "",
                "city": "",
                "firstName": "",
                "lastName": "",
                "fullName": "",
                "country": "",
                "stateCode": "",
                "addressLine1": "",
                "addressLine2": "",
                "postalCode": "",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "INSTALLAT"
                  }
                ]
              },
              {
                "companyName": "Qantas Airways Limited",
                "cityCode": "",
                "city": "Mascot",
                "firstName": "Vinay",
                "lastName": "Kumar",
                "fullName": "Vinay Kumar",
                "country": "AU",
                "stateCode": "",
                "addressLine1": "10 Bourke Road",
                "addressLine2": "",
                "postalCode": "2020",
                "customerNum": "D11200523232",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "SHIPPING"
                  }
                ]
              },
              {
                "companyName": "",
                "cityCode": "",
                "city": "",
                "firstName": "",
                "lastName": "",
                "fullName": "",
                "country": "",
                "stateCode": "",
                "addressLine1": "",
                "addressLine2": "",
                "postalCode": "",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "DELIVERY"
                  }
                ]
              },
              {
                "companyName": "",
                "cityCode": "",
                "city": "",
                "firstName": "",
                "lastName": "",
                "fullName": "",
                "country": "",
                "stateCode": "",
                "addressLine1": "",
                "addressLine2": "",
                "postalCode": "",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "DELIVERY"
                  }
                ]
              },
              {
                "companyName": "Qantas Airways Limited",
                "cityCode": "",
                "city": "Mascot",
                "firstName": "Vinay",
                "lastName": "Kumar",
                "fullName": "Vinay Kumar",
                "country": "AU",
                "stateCode": "NSW",
                "addressLine1": "10 Bourke Road",
                "addressLine2": "",
                "postalCode": "2020",
                "customerNum": "P10013040501",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "END_USER"
                  }
                ]
              },
              {
                "companyName": "",
                "cityCode": "",
                "city": "",
                "firstName": "",
                "lastName": "",
                "fullName": "",
                "country": "",
                "stateCode": "",
                "addressLine1": "",
                "addressLine2": "",
                "postalCode": "",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "INSTALLAT"
                  }
                ]
              },
              {
                "companyName": "Qantas Airways Limited",
                "cityCode": "",
                "city": "Mascot",
                "firstName": "Vinay",
                "lastName": "Kumar",
                "fullName": "",
                "country": "AU",
                "stateCode": "",
                "addressLine1": "10 Bourke Road",
                "addressLine2": "",
                "postalCode": "2020",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "SOLDTO"
                  }
                ]
              },
              {
                "companyName": "",
                "cityCode": "",
                "city": "",
                "firstName": "",
                "lastName": "",
                "fullName": "",
                "country": "",
                "stateCode": "",
                "addressLine1": "",
                "addressLine2": "",
                "postalCode": "",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "DELIVERY"
                  }
                ]
              },
              {
                "companyName": "Qantas Airways Limited",
                "cityCode": "",
                "city": "Mascot",
                "firstName": "Vinay",
                "lastName": "Kumar",
                "fullName": "Vinay Kumar",
                "country": "AU",
                "stateCode": "",
                "addressLine1": "10 Bourke Road",
                "addressLine2": "",
                "postalCode": "2020",
                "customerNum": "D11200523232",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "SHIPPING"
                  }
                ]
              },
              {
                "companyName": "Qantas Airways Limited",
                "cityCode": "",
                "city": "Mascot",
                "firstName": "Vinay",
                "lastName": "Kumar",
                "fullName": "Vinay Kumar",
                "country": "AU",
                "stateCode": "",
                "addressLine1": "10 Bourke Road",
                "addressLine2": "",
                "postalCode": "2020",
                "customerNum": "",
                "customerNameExt": "",
                "contact": [
                  {
                    "contactType": "BILLING"
                  }
                ]
              }
            ],
            "fulfillments": [
              {
                "deliveryCity": "",
                "mergeType": "PTO-DIRECT",
                "paymentTerm": "IMMEDIATE",
                "shipCode": "IY",
                "systemQty": "1",
                "shipByDate": "",
                "mustArriveByDate": "",
                "manifestDate": "",
                "revisedDeliveryDate": "",
                "oicId": "ZTmNBGxMEfCHJe_Jjx4dAA",
                "fulfillmentId": "3000070327",
                "address": [
                  {
                    "postalCode": "2020",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "SHIPPING"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": "61-02261111818"
                      }
                    ]
                  },
                  {
                    "postalCode": "",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "DELIVERY"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": ""
                      }
                    ]
                  },
                  {
                    "postalCode": "2020",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "END_USER"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": "61-02261111818"
                      }
                    ]
                  },
                  {
                    "postalCode": "",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "INSTALLAT"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": ""
                      }
                    ]
                  }
                ],
                "soStatus": [
                  {
                    "sourceSystemStsCode": "SC",
                    "fulfillmentStsCode": "Ship Confirm",
                    "statusDate": "2025-07-29 07:35:16.637013"
                  }
                ],
                "salesOrderLines": [
                  {
                    "soLineQty": "1",
                    "lob": "Dell UltraSharp Monitors",
                    "siNumber": "",
                    "soLineNum": "1",
                    "facility": "BX2",
                    "specialInstructions": []
                  }
                ],
                "fulfillmentOrder": [
                  {
                    "foId": "7355860096558333953",
                    "fulfillmentId": "3000070327"
                  }
                ]
              },
              {
                "deliveryCity": "",
                "mergeType": "ATO-INDIRECT",
                "paymentTerm": "IMMEDIATE",
                "shipCode": "IY",
                "systemQty": "5",
                "shipByDate": "",
                "mustArriveByDate": "",
                "manifestDate": "",
                "revisedDeliveryDate": "",
                "oicId": "_CiIpGxVEfCLGBGxs3YlMQ",
                "fulfillmentId": "3000070326",
                "address": [
                  {
                    "postalCode": "",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "INSTALLAT"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": ""
                      }
                    ]
                  },
                  {
                    "postalCode": "2020",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "END_USER"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": "61-02261111818"
                      }
                    ]
                  },
                  {
                    "postalCode": "",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "INSTALLAT"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": ""
                      }
                    ]
                  },
                  {
                    "postalCode": "",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "DELIVERY"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": ""
                      }
                    ]
                  },
                  {
                    "postalCode": "",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "DELIVERY"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": ""
                      }
                    ]
                  },
                  {
                    "postalCode": "2020",
                    "taxRegstrnNum": "",
                    "contact": [
                      {
                        "contactType": "SHIPPING"
                      }
                    ],
                    "phone": [
                      {
                        "phoneNumber": "61-02261111818"
                      }
                    ]
                  }
                ],
                "soStatus": [
                  {
                    "sourceSystemStsCode": "IP",
                    "fulfillmentStsCode": "In Production",
                    "statusDate": "2025-07-29 13:20:07.113703"
                  }
                ],
                "salesOrderLines": [
                  {
                    "soLineQty": "5",
                    "lob": "",
                    "siNumber": "",
                    "soLineNum": "23",
                    "facility": "",
                    "specialInstructions": []
                  },
                  {
                    "soLineQty": "5",
                    "lob": "Inspiron 14 5440",
                    "siNumber": "",
                    "soLineNum": "1",
                    "facility": "WCD",
                    "specialInstructions": []
                  }
                ],
                "fulfillmentOrder": [
                  {
                    "foId": "7355928803984830465",
                    "fulfillmentId": "3000070326"
                  }
                ]
              }
            ],
            "salesRep": [
              {
                "salesRepName": ""
              }
            ]
          }
        ]
      }
    }
  },
  {
    "data": {
      "getWorkOrderByWoIds": [
        {
          "channel": "GOLF",
          "vendorSiteId": "BX2",
          "dellBlanketPoNum": "",
          "woLines": [],
          "woType": "PICK",
          "woId": "1122192942",
          "woStatusList": [
            {
              "channelStatusCode": "3000"
            },
            {
              "channelStatusCode": "1000"
            },
            {
              "channelStatusCode": "2000"
            }
          ],
          "woShipInstr": [],
          "shipMode": "AR",
          "shipToFacility": "CUST"
        },
        {
          "channel": "BOSS",
          "vendorSiteId": "WCD",
          "dellBlanketPoNum": "",
          "woLines": [
            {
              "ismultipack": "",
              "woLineType": "MAKE"
            }
          ],
          "woType": "MAKE",
          "woId": "1127194521",
          "woStatusList": [
            {
              "channelStatusCode": "2000"
            },
            {
              "channelStatusCode": "3000"
            },
            {
              "channelStatusCode": "6500"
            },
            {
              "channelStatusCode": "4000"
            },
            {
              "channelStatusCode": "1000"
            },
            {
              "channelStatusCode": "3800"
            },
            {
              "channelStatusCode": "5000"
            }
          ],
          "woShipInstr": [],
          "shipMode": "AR",
          "shipToFacility": "BX2"
        },
        {
          "channel": "GOLF",
          "vendorSiteId": "BX2",
          "dellBlanketPoNum": "",
          "woLines": [
            {
              "ismultipack": "",
              "woLineType": "PART_PICK"
            },
            {
              "ismultipack": "",
              "woLineType": "PART_PICK"
            },
            {
              "ismultipack": "",
              "woLineType": "PART_PICK"
            }
          ],
          "woType": "PICK",
          "woId": "1888314027",
          "woStatusList": [
            {
              "channelStatusCode": "2000"
            },
            {
              "channelStatusCode": "4000"
            },
            {
              "channelStatusCode": "1000"
            },
            {
              "channelStatusCode": "8050"
            },
            {
              "channelStatusCode": "8000"
            },
            {
              "channelStatusCode": "5000"
            },
            {
              "channelStatusCode": "3000"
            }
          ],
          "woShipInstr": [],
          "shipMode": "IY",
          "shipToFacility": "CUST"
        }
      ]
    }
  }
]


here under the fullfillment is coming 2 data but respose is coming first data of fullfillment for all  response 

if fulfillmentOrder is empty list []
mean have to take fulfillmentId from under fullfillments else fulfillmentId take from fullfillments -> fulfillmentOrder -> fulfillmentId

same like foId
