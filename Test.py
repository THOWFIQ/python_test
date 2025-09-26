def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    print(json.dumps(result_map,indent=2))
    # print(json.dumps(result_map.get("graphql_details"),indent=2))
    exit()
    try:
        flat_list = []
        # ValidCount = []

        # sales_wo_details contains the mapping of SalesOrder -> WOs
        # sales_wo_details = result_map.get("sales_orders_summary", [])
        # graphql_details = result_map.get("graphql_details", [])

        # Build a mapping for quick lookup of WO IDs by Sales Order ID
        # so_wo_map = {so['salesOrderId']: so.get('workOrderIds', []) for so in sales_wo_details}

        for item in result_map:
           
            data = item.get("data", {})
          
            workorders_Data = data.get("getWorkOrderByWoIds", [])
            ffids_Data = data.get("getSalesOrderByFfids",{}).get("salesOrders",[])
            print("\n")
            print("wo data ")
            # print(ffids_Data)
            print("\n")

            # if workorders_Data:
            #     continue
            
            # if not ffids_Data:
            #     continue

            if len(workorders_Data) >0:
                print("coming")
                wo_row = {
                        "WO_ID": safe_get(workorders_Data, ['woId']),
                        "Dell Blanket PO Num": safe_get(workorders_Data, ['dellBlanketPoNum']),
                        "Ship To Facility": safe_get(workorders_Data, ['shipToFacility']),
                        "Is Last Leg": 'Y' if safe_get(workorders_Data, ['shipToFacility']) else 'N',
                        "Ship From MCID": safe_get(workorders_Data, ['vendorSiteId']),
                        "WO OTM Enabled": safe_get(workorders_Data, ['isOtmEnabled']),
                        "WO Ship Mode": safe_get(workorders_Data, ['shipMode']),
                        "Is Multipack": safe_get(workorders_Data, ['woLines', 0, 'ismultipack']),
                        "Has Software": any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in safe_get(workorders_Data, ['woLines']) or []),
                        "Make WO Ack Date": next(
                            (dateFormation(status.get("statusDate"))
                                for status in workorders_Data.get("woStatusList", [])
                                if str(status.get("channelStatusCode")) == "3000" and workorders_Data.get("woType") == "MAKE"),
                            ""
                        ),
                        "MCID Value": (
                            safe_get(workorders_Data, ['woShipInstr', 0, "mergeFacility"]) or
                            safe_get(workorders_Data, ['woShipInstr', 0, "carrierHubCode"])
                        )
                    }
                print(wo_row)
                continue
            elif len(ffids_Data) >0:
                print("coming el if part")
                continue

                # print(wo_row)
            # if ffids_Data:
            #     print(ffids_Data)
                # exit()

        #         sales_order_id = safe_get(so, ['salesOrderId'])
        #         # if region and region.upper() != safe_get(so, ['region'], "").upper():
        #         #     continue

        #         # if filtersValue:
        #         #     ValidCount.append(sales_order_id)

        #         fulfillments = safe_get(so, ['fulfillments']) or []
        #         # print(len(fulfillments))
        #         # print(json.dumps(fulfillments,indent=2))
        #         # exit()
        #         if isinstance(fulfillments, dict):
        #             fulfillments = [fulfillments]

        #         shipping_addr = pick_address_by_type(so, "SHIPPING")
        #         billing_addr = pick_address_by_type(so, "BILLING")
        #         shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING") if fulfillments else None
        #         shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

        #         lob_list = list(filter(
        #             lambda lob: lob and lob.strip() != "",
        #             map(lambda line: safe_get(line, ['lob']), safe_get(fulfillments, [0,'salesOrderLines']) or [])
        #         ))
        #         lob = ", ".join(lob_list)

        #         facility_list = list(filter(
        #             lambda f: f and f.strip() != "",
        #             map(lambda line: safe_get(line, ['facility']), safe_get(fulfillments, [0,'salesOrderLines']) or [])
        #         ))
        #         facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f))

        #         def get_status_date(code):
        #             status_code = safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode'])
        #             if status_code == code:
        #                 return dateFormation(safe_get(fulfillments, [0, 'soStatus', 0, 'statusDate']))
        #             return ""

        #         # Base row for Sales Order (WO fields empty by default)
        #         row = {
        #             "Fulfillment ID": safe_get(fulfillments, [0, 'fulfillmentId']),
        #             "BUID": safe_get(so, ['buid']),
        #             "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
        #             "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
        #             "LOB": lob,
        #             "Sales Order ID": sales_order_id,
        #             "Agreement ID": safe_get(so, ['agreementId']),
        #             "Amount": safe_get(so, ['totalPrice']),
        #             "Currency Code": safe_get(so, ['currency']),
        #             "Customer Po Number": safe_get(so, ['poNumber']),
        #             "Delivery City": safe_get(fulfillments, [0, 'deliveryCity']),
        #             "DOMS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
        #             "Dp ID": safe_get(so, ['dpid']),
        #             "Fulfillment Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
        #             "Merge Type": safe_get(fulfillments, [0, 'mergeType']),
        #             "InstallInstruction2": get_install_instruction2_id(so),
        #             "PP Date": get_status_date("PP"),
        #             "IP Date": get_status_date("IP"),
        #             "MN Date": get_status_date("MN"),
        #             "SC Date": get_status_date("SC"),
        #             "Location Number": safe_get(so, ['locationNum']),
        #             "OFS Status Code": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
        #             "OFS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
        #             "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
        #             "ShippingContactName": shipping_contact_name,
        #             "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
        #             "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
        #             "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
        #             "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
        #             "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
        #             "ShipToPhone": (listify(shipping_phone.get("phone", []))[0].get("phoneNumber", "")
        #                             if shipping_phone and listify(shipping_phone.get("phone", [])) else ""),
        #             "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
        #             "Order Age": safe_get(so, ['orderDate']),
        #             "Order Amount usd": safe_get(so, ['rateUsdTransactional']),
        #             "Rate Usd Transactional": safe_get(so, ['rateUsdTransactional']),
        #             "Sales Rep Name": safe_get(so, ['salesrep', 0, 'salesRepName']),
        #             "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
        #             "Source System Status": safe_get(fulfillments, [0, 'soStatus', 0,'sourceSystemStsCode']),
        #             "Tie Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'soLineNum']),
        #             "Si Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'siNumber']),
        #             "Req Ship Code": safe_get(fulfillments, [0, 'shipCode']),
        #             "Reassigned IP Date": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
        #             "Payment Term Code": safe_get(fulfillments, [0, 'paymentTerm']),
        #             "Region Code": safe_get(so, ['region']),
        #             "FO ID": safe_get(fulfillments, [0, 'fulfillmentOrder', 0, 'foId']),
        #             "System Qty": safe_get(fulfillments, [0, 'systemQty']),
        #             "Ship By Date": safe_get(fulfillments, [0, 'shipByDate']),
        #             "Facility": facility,
        #             "Tax Regstrn Num": safe_get(fulfillments, [0, 'address', 0, 'taxRegstrnNum']),
        #             "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
        #             "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
        #             "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
        #             "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
        #             "Country": shipping_addr.get("country", "") if shipping_addr else "",
        #             "Ship Code": safe_get(fulfillments, [0, 'shipCode']),
        #             "Must Arrive By Date": dateFormation(safe_get(fulfillments, [0, 'mustArriveByDate'])),
        #             "Manifest Date": dateFormation(safe_get(fulfillments, [0, 'manifestDate'])),
        #             "Revised Delivery Date": dateFormation(safe_get(fulfillments, [0, 'revisedDeliveryDate'])),
        #             "Source System ID": safe_get(so, ['sourceSystemId']),
        #             "OIC ID": safe_get(fulfillments, [0, 'oicId']),
        #             "Order Date": dateFormation(safe_get(so, ['orderDate'])),
        #             "Order Type": dateFormation(safe_get(so, ['orderType'])),
        #             "WO_ID": "",
        #             "Dell Blanket PO Num": "",
        #             "Ship To Facility": "",
        #             "Is Last Leg": "",
        #             "Ship From MCID": "",
        #             "WO OTM Enabled": "",
        #             "WO Ship Mode": "",
        #             "Is Multipack": "",
        #             "Has Software": "",
        #             "Make WO Ack Date": "",
        #             "MCID Value": ""
        #         }
        #         if wo_row:
        #             flat_list.append({**row, **wo_row})
        #         else:
        #             flat_list.append(row)
        #     # wo_ids = so_wo_map.get(sales_order_id, [])
        #     # if wo_ids:
        #     #     for WO_ID in wo_ids:
        #     #         wo_obj = next((wo for wo in raw_workorders if wo.get("woId") == WO_ID), {})
        #     #         wo_row = {
        #     #             "WO_ID": WO_ID,
        #     #             "Dell Blanket PO Num": safe_get(wo_obj, ['dellBlanketPoNum']),
        #     #             "Ship To Facility": safe_get(wo_obj, ['shipToFacility']),
        #     #             "Is Last Leg": 'Y' if safe_get(wo_obj, ['shipToFacility']) else 'N',
        #     #             "Ship From MCID": safe_get(wo_obj, ['vendorSiteId']),
        #     #             "WO OTM Enabled": safe_get(wo_obj, ['isOtmEnabled']),
        #     #             "WO Ship Mode": safe_get(wo_obj, ['shipMode']),
        #     #             "Is Multipack": safe_get(wo_obj, ['woLines', 0, 'ismultipack']),
        #     #             "Has Software": any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in safe_get(wo_obj, ['woLines']) or []),
        #     #             "Make WO Ack Date": next(
        #     #                 (dateFormation(status.get("statusDate"))
        #     #                     for status in wo_obj.get("woStatusList", [])
        #     #                     if str(status.get("channelStatusCode")) == "3000" and wo_obj.get("woType") == "MAKE"),
        #     #                 ""
        #     #             ),
        #     #             "MCID Value": (
        #     #                 safe_get(wo_obj, ['woShipInstr', 0, "mergeFacility"]) or
        #     #                 safe_get(wo_obj, ['woShipInstr', 0, "carrierHubCode"])
        #     #             )
        #     #         }
        #     #         flat_list.append({**row, **wo_row})
        #     # else:
        #     #     flat_list.append(row)
           

        # count_valid = len(ValidCount)
        # if not flat_list:
        #     return {"error": "No Data Found"}

        # if format_type == "export":
        #     data = [{"Count ": count_valid}, flat_list] if filtersValue else flat_list
        #     ValidCount.clear()
        #     return data

        # elif format_type == "grid":
        #     desired_order = list(flat_list[0].keys())
        #     rows = []
        #     for item in flat_list:
        #         row = {"columns": [{"value": item.get(k, "")} for k in desired_order]}
        #         rows.append(row)
        #     table_grid_output = tablestructural(rows, region) if rows else []
        #     if filtersValue:
        #         table_grid_output["Count"] = count_valid
        #     ValidCount.clear()
        #     return table_grid_output

        # return flat_list

    except Exception as e:
        return {"error": str(e)}
