def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
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
        sales_wo_details = result_map.get("sales_orders_summary", [])
        graphql_details = result_map.get("graphql_details", [])

        for item_index, item in enumerate(graphql_details):
            if not isinstance(item, dict):
                print(f"Item index: {item_index} type: {type(item)}")
                print(f"Skipping non-dict item: {item}")
                continue

            data = item.get("data", {})
            if not data:
                continue

            sales_orders = extract_sales_order(data)
            workorders = data.get("getWorkOrderByWoIds", [])
           
            if sales_orders:                
                for so in sales_orders:                                
                    fulfillments = safe_get(so, ['fulfillments']) or []
                    if isinstance(fulfillments, dict):
                        fulfillments = [fulfillments]

                    if filtersValue:
                        sales_order_id = safe_get(so, ['salesOrderId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(sales_order_id)

                    if region and region.upper() != safe_get(so, ['region'], "").upper():
                        continue
                    
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
                    
                    row = {
                        "Fulfillment ID": safe_get(fulfillments, [0, 'fulfillmentId']),
                        "BUID": safe_get(so, ['buid']),
                        "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                        "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                        "LOB": lob,
                        "Sales Order ID": safe_get(so, ['salesOrderId']),
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

            if workorders:
                for WorkOrderData in workorders:
                    # SO_ID = safe_get(WorkOrderData, ['woId'])
                    WO_ID = safe_get(WorkOrderData, ['woId'])
                    DellBlanketPoNum = safe_get(WorkOrderData, ['dellBlanketPoNum'])
                    ship_to_facility = safe_get(WorkOrderData, ['shipToFacility'])
                    IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
                    ShipFromMcid = safe_get(WorkOrderData, ['vendorSiteId'])
                    WoOtmEnable = safe_get(WorkOrderData, ['isOtmEnabled'])
                    WoShipMode = safe_get(WorkOrderData, ['shipMode'])
                    ismultipack = safe_get(WorkOrderData, ['woLines', 0, "ismultipack"])
                    wo_lines = safe_get(WorkOrderData, ['woLines'])
                    has_software = any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in wo_lines)
                    MakeWoAckValue = next(
                        (
                            dateFormation(status.get("statusDate"))
                            for status in WorkOrderData.get("woStatusList", [])
                            if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"
                        ),
                        ""
                    )
                    McidValue = (
                                    safe_get(WorkOrderData, ['woShipInstr', 0, "mergeFacility"]) or
                                    safe_get(WorkOrderData, ['woShipInstr', 0, "carrierHubCode"])
                                )
                    wo_row = {
                        "WO_ID": WO_ID,
                        "Dell Blanket PO Num": DellBlanketPoNum,
                        "Ship To Facility": ship_to_facility,
                        "Is Last Leg": IsLastLeg,
                        "Ship From MCID": ShipFromMcid,
                        "WO OTM Enabled": WoOtmEnable,
                        "WO Ship Mode": WoShipMode,
                        "Is Multipack": ismultipack,
                        "Has Software": has_software,
                        "Make WO Ack Date": MakeWoAckValue,
                        "MCID Value": McidValue
                    }
                    # merged_row = {**row,**wo_row}
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
