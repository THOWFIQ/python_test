def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    # print(json.dumps(result_map,indent=2))
    # exit()
    try:
        def extract_sales_order(data):
            if not data or not isinstance(data, dict):
                return None

            soids_data = data.get("getSalesOrderBySoids")
            woids_data = data.get('getWorkOrderByWoIds')
            if soids_data:
                sales_orders = soids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders,woids_data

            ffids_data = data.get("getSalesOrderByFfids")
            if ffids_data:
                sales_orders = ffids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders

            # woids_data = data.get('getWorkOrderByWoIds')
            # if woids_data:
            #     return woids_data

            return None
        
        flat_list = []
        ValidCount = []
        
        for item in result_map:            
            data = item.get("data")
           
            if not data:
                continue

            soids_data = data.get("getSalesOrderBySoids")
            ffids_data = data.get("getSalesOrderByFfids")
            # woids_data = data.get("getWorkOrderByWoIds")
            # print(json.dumps(woids_data,indent=2))
            # exit()
             # Handle WorkOrders (safe lookup)
            

            sales_orders = extract_sales_order(data)
            print(json.dumps(sales_orders,indent=2))
            exit()
            if not sales_orders or len(sales_orders) == 0:
                continue

            for so in sales_orders:
                fulfillments = safe_get(so, ['fulfillments'])

                if isinstance(fulfillments, dict):
                    fulfillments = [fulfillments]

                if filtersValue:
                    if soids_data and not ffids_data:
                        sales_order_id = safe_get(so, ['salesOrderId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(sales_order_id)
                            # print("Appended sales_order_id:", sales_order_id)
                            # print("Current ValidCount:", ValidCount)
                    elif ffids_data and not soids_data:
                        fulfillment_id = safe_get(fulfillments, [0, 'fulfillmentId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(fulfillment_id)
                            # print("Appended fulfillment_id:", fulfillment_id)
                            # print("Current ValidCount:", ValidCount)
                    elif woids_data and not soids_data and not ffids_data:
                        work_order_id = safe_get(fulfillments, [0, 'fulfillmentId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(fulfillment_id)
                            # print("Appended fulfillment_id:", fulfillment_id)
                            # print("Current ValidCount:", ValidCount)
                # else:
                #     print("Both soids_data and ffids_data present, skipping append")
                    # pass
                
                if region and region.upper() != safe_get(so, ['region'], "").upper():
                    continue
                shipping_addr = pick_address_by_type(so, "SHIPPING")
                shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING")
                billing_addr = pick_address_by_type(so, "BILLING")
                shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""
                              
                lob_list = list(filter(
                            lambda lob: lob is not None and lob.strip() != "",
                            map(
                                lambda line: safe_get(line, ['lob']),
                                safe_get(fulfillments, [0,'salesOrderLines']) or []
                            )
                        ))
                
                lob = ", ".join(lob_list)

                facility_list = list(filter(
                            lambda facility: facility is not None and facility.strip() != "",
                            map(
                                lambda line: safe_get(line, ['facility']),
                                safe_get(fulfillments, [0,'salesOrderLines']) or []
                            )
                        ))

                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f and f.strip()))

                def get_status_date(code):
                    status_code = safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(fulfillments, [0, 'soStatus', 0, 'statusDate']))
                    return ""

                # WO_ID = safe_get(WorkOrderData, ['woId'])
                # DellBlanketPoNum = safe_get(WorkOrderData, ['dellBlanketPoNum'])
                # ship_to_facility = safe_get(WorkOrderData, ['shipToFacility'])
                # IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
                # ShipFromMcid = safe_get(WorkOrderData, ['vendorSiteId'])
                # WoOtmEnable = safe_get(WorkOrderData, ['isOtmEnabled'])
                # WoShipMode = safe_get(WorkOrderData, ['shipMode'])
                # ismultipack = safe_get(WorkOrderData, ['woLines',0,"ismultipack"])
                # wo_lines = safe_get(WorkOrderData, ['woLines'])
                # has_software = any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in wo_lines)
                # MakeWoAckValue = next((dateFormation(status.get("statusDate")) for status in WorkOrderData.get("woStatusList", [])
                #                         if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"),
                #                         "")
                # McidValue = (
                #                 WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                #                 WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                #             )
                
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
                    "Order Type": dateFormation(safe_get(so, ['orderType']))
                }

                flat_list.append(row)

        count_valid = len(ValidCount)

        if not flat_list:
            return {"error": "No Data Found"}
       
        if len(flat_list) > 0:
            if format_type == "export":
                if filtersValue:
                    data = []
                    count =  {"Count ": count_valid}
                    data.append(count)
                    data.append(flat_list)
                    ValidCount.clear()
                    return data
                else:
                    return flat_list

            elif format_type == "grid":
                
                desired_order = [
                    "Fulfillment ID","BUID","BillingCustomerName","CustomerName","LOB","Sales Order ID","Agreement ID",
                    "Amount","Currency Code","Customer Po Number","Delivery City","DOMS Status","Dp ID","Fulfillment Status",
                    "Merge Type","InstallInstruction2","PP Date","IP Date","MN Date","SC Date","Location Number","OFS Status Code",
                    "OFS Status","ShippingCityCode","ShippingContactName","ShippingCustName","ShippingStateCode","ShipToAddress1",
                    "ShipToAddress2","ShipToCompany","ShipToPhone","ShipToPostal","Order Age","Order Amount usd","Rate Usd Transactional",
                    "Sales Rep Name","Shipping Country","Source System Status","Tie Number","Si Number","Req Ship Code",
                    "Reassigned IP Date","Payment Term Code","Region Code","FO ID","System Qty","Ship By Date","Facility",
                    "Tax Regstrn Num","State Code","City Code","Customer Num","Customer Name Ext","Country","Ship Code",
                    "Must Arrive By Date","Manifest Date","Revised Delivery Date","Source System ID","OIC ID","Order Date","Order Type"
                ]
                
                rows = []
                for item in flat_list:
                    reordered_values = [item.get(key, "") for key in desired_order]
                    row = {"columns": [{"value": val if val is not None else ""} for val in reordered_values]}
                    rows.append(row)
                table_grid_output = tablestructural(rows,region) if rows else []
                
                if filtersValue:
                    table_grid_output["Count"] = count_valid
                ValidCount.clear()
                return table_grid_output

    except Exception as e:
        return {"error": str(e)}
