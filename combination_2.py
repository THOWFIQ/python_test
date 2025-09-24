flat_list = []
ValidCount = []

for item in result_map:
    data = item.get("data")
    if not data:
        continue

    # --- Sales Orders ---
    sales_orders = data.get("getSalesOrderBySoids", {}).get("salesOrders", [])
    for so in sales_orders:
        fulfillments = listify(so.get("fulfillments", []))
        workorders_from_so = listify(so.get("workOrders", []))  # in case wo inside so
        # Process sales order row here (your row building code)
        row = {
            "Sales Order ID": so.get("salesOrderId"),
            "BUID": so.get("buid"),
            "Amount": so.get("totalPrice"),
            "Currency Code": so.get("currency"),
            # ... add all your fields ...
        }
        flat_list.append(row)

    # --- Work Orders ---
    work_orders = data.get("getWorkOrderByWoIds", [])
    for wo in work_orders:
        WO_ID = wo.get("woId")
        DellBlanketPoNum = wo.get("dellBlanketPoNum")
        ship_to_facility = wo.get("shipToFacility")


           results = asyncio.run(run_all(graphql_request))

    # Return both detailed GraphQL results and summary of Sales Orders
    return {
        "sales_orders_summary": finalResult,
        "graphql_details": results
    }
        IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
        ShipFromMcid = wo.get("vendorSiteId")
        WoOtmEnable = wo.get("isOtmEnabled")
        WoShipMode = wo.get("shipMode")
        wo_lines = wo.get("woLines", [])
        ismultipack = wo_lines[0].get("ismultipack") if wo_lines else ""
        has_software = any(line.get("woLineType") == "SOFTWARE" for line in wo_lines)
        MakeWoAckValue = next(
            (status.get("statusDate") for status in wo.get("woStatusList", [])
             if str(status.get("channelStatusCode")) == "3000" and wo.get("woType") == "MAKE"), ""
        )
        McidValue = (
            wo.get('woShipInstr', [{}])[0].get('mergeFacility') or
            wo.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
        )

        wo_row = {
            "WO_ID": WO_ID,
            "DellBlanketPoNum": DellBlanketPoNum,
            "Ship To Facility": ship_to_facility,
            "Is Last Leg": IsLastLeg,
            "Ship From MCID": ShipFromMcid,
            "Otm Enabled": WoOtmEnable,
            "Ship Mode": WoShipMode,
            "Is Multipack": ismultipack,
            "Has Software": has_software,
            "Make WO Ack": MakeWoAckValue,
            "MCID Value": McidValue
        }
        flat_list.append(wo_row)

for idx, item in enumerate(result_map):
    print("Item index:", idx, "type:", type(item))
    if not isinstance(item, dict):
        print("Skipping non-dict item:", item)
        continue

    data = item.get("data")
    if not data:
        print("No 'data' key in item:", item)
        continue

    print("break point - data exists")

Item index: 0 type: <class 'str'>
Skipping non-dict item: sales_orders_summary
Item index: 1 type: <class 'str'>
Skipping non-dict item: graphql_details

   results = asyncio.run(run_all(graphql_request))

    # Return both detailed GraphQL results and summary of Sales Orders
    return {
        "sales_orders_summary": finalResult,
        "graphql_details": results
    }


def outputformat(result_map, format_type=None, filtersValue=None, region=None):
    try:
        flat_list = []
        ValidCount = []

        # Only use GraphQL details
        graphql_details = result_map.get("graphql_details", [])

        for item in graphql_details:
            data = item.get("data")  # actual GraphQL response
            if not data:
                continue

            # Handle Sales Orders
            sales_orders = data.get("getSalesOrderBySoids", {}).get("salesOrders", [])
            for so in sales_orders:
                fulfillments = so.get("fulfillments", [])
                workorders = []  # We'll attach Work Orders later

                # Collect Work Orders from another GraphQL response
                wo_data_list = data.get("getWorkOrderByWoIds", [])
                workorders.extend(wo_data_list)

                # ------------------- Sales Order Row -------------------
                for fulfillment in fulfillments:
                    row = {
                        "Sales Order ID": so.get("salesOrderId"),
                        "Fulfillment ID": fulfillment.get("fulfillmentId"),
                        "BUID": so.get("buid"),
                        "CustomerName": "",
                        "LOB": ", ".join([line.get("lob") or "" for line in fulfillment.get("salesOrderLines", [])]),
                        "Facility": ", ".join([line.get("facility") or "" for line in fulfillment.get("salesOrderLines", [])]),
                        "Delivery City": fulfillment.get("deliveryCity"),
                        "Amount": so.get("totalPrice"),
                        "Currency Code": so.get("currency")
                        # add other fields as needed
                    }
                    flat_list.append(row)

                # ------------------- Work Orders Rows -------------------
                for WorkOrderData in workorders:
                    WO_ID = WorkOrderData.get('woId')
                    DellBlanketPoNum = WorkOrderData.get('dellBlanketPoNum')
                    ship_to_facility = WorkOrderData.get('shipToFacility')
                    IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
                    ShipFromMcid = WorkOrderData.get('vendorSiteId')
                    WoOtmEnable = WorkOrderData.get('isOtmEnabled')
                    WoShipMode = WorkOrderData.get('shipMode')
                    wo_lines = WorkOrderData.get('woLines', [])
                    ismultipack = wo_lines[0].get("ismultipack") if wo_lines else ""
                    has_software = any(line.get('woLineType') == 'SOFTWARE' for line in wo_lines)
                    MakeWoAckValue = next(
                        (status.get("statusDate") for status in WorkOrderData.get("woStatusList", [])
                         if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"),
                        ""
                    )
                    McidValue = (
                        WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                        WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                    )

                    wo_row = {
                        "Sales Order ID": so.get("salesOrderId"),
                        "WO_ID": WO_ID,
                        "DellBlanketPoNum": DellBlanketPoNum,
                        "Ship To Facility": ship_to_facility,
                        "Is Last Leg": IsLastLeg,
                        "Ship From MCID": ShipFromMcid,
                        "Otm Enabled": WoOtmEnable,
                        "Ship Mode": WoShipMode,
                        "Is Multipack": ismultipack,
                        "Has Software": has_software,
                        "Make WO Ack": MakeWoAckValue,
                        "MCID Value": McidValue
                    }
                    flat_list.append(wo_row)

        if not flat_list:
            return {"error": "No Data Found"}

        if format_type == "export":
            return flat_list
        elif format_type == "grid":
            return flat_list

        return flat_list

    except Exception as e:
        return {"error": str(e)}


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def OutputFormat(result_map, format_type=None, filtersValue=None, region=None):
    try:
        flat_list = []
        ValidCount = []

        # Only use GraphQL results
        graphql_details = result_map.get("graphql_details", [])

        for item_index, item in enumerate(graphql_details):
            if not isinstance(item, dict):
                print(f"Item index: {item_index} type: {type(item)}")
                print(f"Skipping non-dict item: {item}")
                continue

            data = item.get("data")
            if not data:
                continue

            # ------------------- Sales Orders -------------------
            sales_orders = data.get("getSalesOrderBySoids", {}).get("salesOrders", [])
            for so in sales_orders:
                fulfillments = so.get("fulfillments", [])
                workorders = data.get("getWorkOrderByWoIds", [])  # Work Orders for this Sales Order

                # Track valid Sales Order IDs
                if filtersValue:
                    sales_order_id = so.get("salesOrderId")
                    if region and region.upper() == so.get("region", "").upper():
                        ValidCount.append(sales_order_id)

                # Region filter
                if region and region.upper() != so.get("region", "").upper():
                    continue

                # ------------------- Sales Order + Fulfillment Rows -------------------
                shipping_addr = next(
                    (addr for addr in so.get("address", []) if any(c.get("contactType") == "SHIPPING" for c in addr.get("contact", []))),
                    {}
                )
                billing_addr = next(
                    (addr for addr in so.get("address", []) if any(c.get("contactType") == "BILLING" for c in addr.get("contact", []))),
                    {}
                )

                for fulfillment in fulfillments:
                    def get_status_date(code):
                        status_code = next(iter([s.get("sourceSystemStsCode") for s in fulfillment.get("soStatus", [])]), None)
                        if status_code == code:
                            return next(iter([s.get("statusDate") for s in fulfillment.get("soStatus", [])]), "")
                        return ""

                    lob_list = [line.get("lob") for line in fulfillment.get("salesOrderLines", []) if line.get("lob")]
                    facility_list = [line.get("facility") for line in fulfillment.get("salesOrderLines", []) if line.get("facility")]

                    row = {
                        "Fulfillment ID": fulfillment.get("fulfillmentId"),
                        "BUID": so.get("buid"),
                        "BillingCustomerName": billing_addr.get("companyName", ""),
                        "CustomerName": shipping_addr.get("companyName", ""),
                        "LOB": ", ".join(lob_list),
                        "Sales Order ID": so.get("salesOrderId"),
                        "Agreement ID": so.get("agreementId"),
                        "Amount": so.get("totalPrice"),
                        "Currency Code": so.get("currency"),
                        "Customer Po Number": so.get("poNumber"),
                        "Delivery City": fulfillment.get("deliveryCity"),
                        "DOMS Status": next(iter([s.get("sourceSystemStsCode") for s in fulfillment.get("soStatus", [])]), ""),
                        "Dp ID": so.get("dpid"),
                        "Fulfillment Status": next(iter([s.get("fulfillmentStsCode") for s in fulfillment.get("soStatus", [])]), ""),
                        "Merge Type": fulfillment.get("mergeType"),
                        "InstallInstruction2": "",  # Add if needed
                        "PP Date": get_status_date("PP"),
                        "IP Date": get_status_date("IP"),
                        "MN Date": get_status_date("MN"),
                        "SC Date": get_status_date("SC"),
                        "Location Number": so.get("locationNum"),
                        "ShippingCityCode": shipping_addr.get("cityCode", ""),
                        "ShippingContactName": shipping_addr.get("fullName", ""),
                        "Facility": ", ".join(facility_list),
                        "Region Code": so.get("region"),
                        "Order Date": so.get("orderDate"),
                        "Order Type": so.get("orderType")
                        # Add any other fields exactly as in your original code
                    }
                    flat_list.append(row)

                # ------------------- Work Orders -------------------
                for WorkOrderData in workorders:
                    WO_ID = WorkOrderData.get('woId')
                    DellBlanketPoNum = WorkOrderData.get('dellBlanketPoNum')
                    ship_to_facility = WorkOrderData.get('shipToFacility')
                    IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
                    ShipFromMcid = WorkOrderData.get('vendorSiteId')
                    WoOtmEnable = WorkOrderData.get('isOtmEnabled')
                    WoShipMode = WorkOrderData.get('shipMode')
                    wo_lines = WorkOrderData.get('woLines', [])
                    ismultipack = wo_lines[0].get("ismultipack") if wo_lines else ""
                    has_software = any(line.get('woLineType') == 'SOFTWARE' for line in wo_lines)
                    MakeWoAckValue = next(
                        (status.get("statusDate") for status in WorkOrderData.get("woStatusList", [])
                         if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"),
                        ""
                    )
                    McidValue = (
                        WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                        WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                    )

                    wo_row = {
                        "Sales Order ID": so.get("salesOrderId"),
                        "WO_ID": WO_ID,
                        "DellBlanketPoNum": DellBlanketPoNum,
                        "Ship To Facility": ship_to_facility,
                        "Is Last Leg": IsLastLeg,
                        "Ship From MCID": ShipFromMcid,
                        "Otm Enabled": WoOtmEnable,
                        "Ship Mode": WoShipMode,
                        "Is Multipack": ismultipack,
                        "Has Software": has_software,
                        "Make WO Ack": MakeWoAckValue,
                        "MCID Value": McidValue
                    }
                    flat_list.append(wo_row)

        if not flat_list:
            return {"error": "No Data Found"}

        # Export or Grid output
        if format_type in ["export", "grid"]:
            return flat_list

        return flat_list

    except Exception as e:
        return {"error": str(e)}
def extract_sales_order(data):
            if not data or not isinstance(data, dict):
                return None, None

            soids_data = data.get("getSalesOrderBySoids")
            woids_data = data.get("getWorkOrderByWoIds")

            if soids_data:
                sales_orders = soids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders, woids_data

            ffids_data = data.get("getSalesOrderByFfids")
            if ffids_data:
                sales_orders = ffids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders, None

            return None, None
========================================================================================================================================================================

def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    # print(json.dumps(result_map,indent=2))
    # exit()
    try:
        def extract_sales_order(data):
            if not data or not isinstance(data, dict):
                return None, None

            soids_data = data.get("getSalesOrderBySoids")
            woids_data = data.get("getWorkOrderByWoIds")

            if soids_data:
                sales_orders = soids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders, woids_data

            ffids_data = data.get("getSalesOrderByFfids")
            if ffids_data:
                sales_orders = ffids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders, None

            return None, None
        
        flat_list = []
        ValidCount = []
       
        # Only use GraphQL results
        graphql_details = result_map.get("graphql_details", [])
        # print(json.dumps(graphql_details,indent=2))
        # exit()
        for item_index, item in enumerate(graphql_details):
            # print(json.dumps(item_index,indent=2))
            # exit()
            if not isinstance(item, dict):
                print(f"Item index: {item_index} type: {type(item)}")
                print(f"Skipping non-dict item: {item}")
                continue

            data = item.get("data")
            if not data:
                continue
            # print(data)
            # exit()
            soids_data = data.get("getSalesOrderBySoids")
            ffids_data = data.get("getSalesOrderByFfids")
            workorders = data.get("getWorkOrderByWoIds")
            print(workorders)
            exit()

            sales_orders = extract_sales_order(data)
            
            # print(json.dumps(sales_orders,indent=2))
            # exit()
            if not sales_orders or len(sales_orders) == 0:
                continue
            # print("hellooo ")
            # print(sales_orders)
            # exit()
            for so in sales_orders:
                # print("coming to for looppppp ")
                # print(json.dumps(so,indent=2))
                # exit()
                fulfillments = safe_get(so, [0,'fulfillments'])
                # fulfillments = listify(safe_get(so, [0,'fulfillments']))
                # workorders = data.get("getWorkOrderByWoIds", [])
                # print(workorders)
                # exit()
                # print(fulfillments)
                # exit()
                if isinstance(fulfillments, dict):
                    fulfillments = [fulfillments]
                # print(fulfillments)
                # exit()
                if filtersValue:
                    if soids_data and not ffids_data:
                        sales_order_id = safe_get(so, [0,'salesOrderId'])
                        if region and region.upper() == safe_get(so, [0,'region'], "").upper():
                            ValidCount.append(sales_order_id)
                            # print("Appended sales_order_id:", sales_order_id)
                            # print("Current ValidCount:", ValidCount)
                    elif ffids_data and not soids_data:
                        fulfillment_id = safe_get(fulfillments, [0, 'fulfillmentId'])
                        if region and region.upper() == safe_get(so, [0,'region'], "").upper():
                            ValidCount.append(fulfillment_id)
                            # print("Appended fulfillment_id:", fulfillment_id)
                            # print("Current ValidCount:", ValidCount)
                    # elif woids_data and not soids_data and not ffids_data:
                    #     work_order_id = safe_get(fulfillments, [0, 'fulfillmentId'])
                    #     if region and region.upper() == safe_get(so, [0,'region'], "").upper():
                    #         ValidCount.append(fulfillment_id)
                            # print("Appended fulfillment_id:", fulfillment_id)
                            # print("Current ValidCount:", ValidCount)
                # else:
                #     print("Both soids_data and ffids_data present, skipping append")
                    # pass
                # print(safe_get(so, [0,'region']))
                # exit()
                if region and region.upper() != safe_get(so, [0,'region'], "").upper():
                    continue
                # print("coming ")
                # exit()
                shipping_addr = pick_address_by_type(so[0], "SHIPPING")
                shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING")
                billing_addr = pick_address_by_type(so[0], "BILLING")               
                shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""
                # print(shipping_contact_name)
                # # exit()
                # print("coming ")
                # exit()
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
                # print("coming ")
                # exit()
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
                
                # print("coming ")
                # exit()
                row = {
                    "Fulfillment ID": safe_get(fulfillments, [0, 'fulfillmentId']),
                    "BUID": safe_get(so, [0,'buid']),
                    "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                    "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "LOB": lob,
                    "Sales Order ID": safe_get(so, [0,'salesOrderId']),
                    "Agreement ID": safe_get(so, [0,'agreementId']),
                    "Amount": safe_get(so, [0,'totalPrice']),
                    "Currency Code": safe_get(so, [0,'currency']),
                    "Customer Po Number": safe_get(so, [0,'poNumber']),
                    "Delivery City": safe_get(fulfillments, [0, 'deliveryCity']),
                    "DOMS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "Dp ID": safe_get(so, [0,'dpid']),
                    "Fulfillment Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                    "Merge Type": safe_get(fulfillments, [0, 'mergeType']),
                    "InstallInstruction2": get_install_instruction2_id(so[0]),
                    "PP Date": get_status_date("PP"),
                    "IP Date": get_status_date("IP"),
                    "MN Date": get_status_date("MN"),
                    "SC Date": get_status_date("SC"),
                    "Location Number": safe_get(so, [0,'locationNum']),
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
                    "Order Age": safe_get(so, [0,'orderDate']),
                    "Order Amount usd": safe_get(so, [0,'rateUsdTransactional']),
                    "Rate Usd Transactional": safe_get(so, [0,'rateUsdTransactional']),
                    "Sales Rep Name": safe_get(so, [0,'salesrep', 0, 'salesRepName']),
                    "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Source System Status": safe_get(fulfillments, [0, 'soStatus', 0,'sourceSystemStsCode']),
                    "Tie Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'soLineNum']),
                    "Si Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'siNumber']),
                    "Req Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                    "Reassigned IP Date": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "Payment Term Code": safe_get(fulfillments, [0, 'paymentTerm']),
                    "Region Code": safe_get(so, [0,'region']),
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
                    "Source System ID": safe_get(so, [0,'sourceSystemId']),
                    "OIC ID": safe_get(fulfillments, [0, 'oicId']),
                    "Order Date": dateFormation(safe_get(so, [0,'orderDate'])),
                    "Order Type": dateFormation(safe_get(so, [0,'orderType']))
                }
                print("coming after row ")
                exit()
                flat_list.append(row)
            # ------------------- Work Orders -------------------
            # for WorkOrderData in workorders:
            #     WO_ID = WorkOrderData.get('woId')
            #     DellBlanketPoNum = WorkOrderData.get('dellBlanketPoNum')
            #     ship_to_facility = WorkOrderData.get('shipToFacility')
            #     IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
            #     ShipFromMcid = WorkOrderData.get('vendorSiteId')
            #     WoOtmEnable = WorkOrderData.get('isOtmEnabled')
            #     WoShipMode = WorkOrderData.get('shipMode')
            #     wo_lines = WorkOrderData.get('woLines', [])
            #     ismultipack = wo_lines[0].get("ismultipack") if wo_lines else ""
            #     has_software = any(line.get('woLineType') == 'SOFTWARE' for line in wo_lines)
            #     MakeWoAckValue = next(
            #         (status.get("statusDate") for status in WorkOrderData.get("woStatusList", [])
            #             if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"),
            #         ""
            #     )
            #     McidValue = (
            #         WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
            #         WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
            #     )

            #     wo_row = {
            #         "Sales Order ID": so.get("salesOrderId"),
            #         "WO_ID": WO_ID,
            #         "DellBlanketPoNum": DellBlanketPoNum,
            #         "Ship To Facility": ship_to_facility,
            #         "Is Last Leg": IsLastLeg,
            #         "Ship From MCID": ShipFromMcid,
            #         "Otm Enabled": WoOtmEnable,
            #         "Ship Mode": WoShipMode,
            #         "Is Multipack": ismultipack,
            #         "Has Software": has_software,
            #         "Make WO Ack": MakeWoAckValue,
            #         "MCID Value": McidValue
            #     }
            #     flat_list.append(wo_row)

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
