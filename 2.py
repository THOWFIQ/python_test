def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    try:
        def extract_sales_order(data):
            """Extract sales orders from data"""
            if not data or not isinstance(data, dict):
                return None
            soids_data = data.get("getSalesOrderBySoids") or data.get("getSalesOrderByFfids")
            if soids_data:
                return soids_data.get("salesOrders")
            return None

        flat_list = []
        ValidCount = []

        graphql_details = result_map.get("graphql_details", [])

        for item_index, item in enumerate(graphql_details):
            if not isinstance(item, dict):
                continue

            data = item.get("data", {})
            if not data:
                continue

            sales_orders = extract_sales_order(data)
            workorders = data.get("getWorkOrderByWoIds", [])

            for so in sales_orders or []:
                # Base SO fields
                shipping_addr = pick_address_by_type(so, "SHIPPING")
                billing_addr = pick_address_by_type(so, "BILLING")
                fulfillments = safe_get(so, ['fulfillments']) or []
                if isinstance(fulfillments, dict):
                    fulfillments = [fulfillments]
                first_fulfillment = fulfillments[0] if fulfillments else {}

                # Collect SO-specific fields
                lob_list = list(filter(None, [safe_get(line, ['lob']) for line in safe_get(first_fulfillment, ['salesOrderLines']) or []]))
                lob = ", ".join(lob_list)

                facility_list = list(filter(None, [safe_get(line, ['facility']) for line in safe_get(first_fulfillment, ['salesOrderLines']) or []]))
                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f))

                def get_status_date(code):
                    status_code = safe_get(first_fulfillment, ['soStatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(first_fulfillment, ['soStatus', 0, 'statusDate']))
                    return ""

                base_row = {
                    "Agreement ID": safe_get(so, ['agreementId']),
                    "Amount": safe_get(so, ['totalPrice']),
                    "BUID": safe_get(so, ['buid']),
                    "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                    "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
                    "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
                    "Customer Po Number": safe_get(so, ['poNumber']),
                    "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "DOMS Status": safe_get(first_fulfillment, ['soStatus', 0, 'sourceSystemStsCode']),
                    "Delivery City": safe_get(first_fulfillment, ['deliveryCity']),
                    "Dp ID": safe_get(so, ['salesOrderId']),
                    "FO ID": safe_get(first_fulfillment, ['fulfillmentOrder', 0, 'foId']),
                    "Facility": facility,
                    "Fulfillment ID": safe_get(first_fulfillment, ['fulfillmentId']),
                    "Fulfillment Status": safe_get(first_fulfillment, ['soStatus', 0, 'fulfillmentStsCode']),
                    "IP Date": get_status_date("IP"),
                    "InstallInstruction2": get_install_instruction2_id(so),
                    "LOB": lob,
                    "Location Number": safe_get(so, ['locationNum']),
                    "MN Date": get_status_date("MN"),
                    "Manifest Date": dateFormation(safe_get(first_fulfillment, ['manifestDate'])),
                    "Merge Type": safe_get(first_fulfillment, ['mergeType']),
                    "Must Arrive By Date": dateFormation(safe_get(first_fulfillment, ['mustArriveByDate'])),
                    "OFS Status": safe_get(first_fulfillment, ['soStatus', 0, 'fulfillmentStsCode']),
                    "OFS Status Code": safe_get(first_fulfillment, ['soStatus', 0, 'sourceSystemStsCode']),
                    "OIC ID": safe_get(first_fulfillment, ['oicId']),
                    "Order Age": safe_get(so, ['orderDate']),
                    "Order Amount usd": safe_get(so, ['rateUsdTransactional']),
                    "Order Date": dateFormation(safe_get(so, ['orderDate'])),
                    "Order Type": safe_get(so, ['orderType']),
                    "PP Date": get_status_date("PP"),
                    "Payment Term Code": safe_get(first_fulfillment, ['paymentTerm']),
                    "Rate Usd Transactional": safe_get(so, ['rateUsdTransactional']),
                    "Reassigned IP Date": safe_get(first_fulfillment, ['soStatus', 0, 'sourceSystemStsCode']),
                    "Region Code": safe_get(so, ['region']),
                    "Req Ship Code": safe_get(first_fulfillment, ['shipCode']),
                    "Revised Delivery Date": dateFormation(safe_get(first_fulfillment, ['revisedDeliveryDate'])),
                    "SC Date": get_status_date("SC"),
                    "Sales Order ID": safe_get(so, ['salesOrderId']),
                    "Sales Rep Name": safe_get(so, ['salesrep', 0, 'salesRepName']),
                    "Ship By Date": safe_get(first_fulfillment, ['shipByDate']),
                    "Ship Code": safe_get(first_fulfillment, ['shipCode']),
                    "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                    "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                    "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShipToPhone": (listify(pick_address_by_type(first_fulfillment, "SHIPPING").get("phone", []))[0].get("phoneNumber", "")
                                    if pick_address_by_type(first_fulfillment, "SHIPPING") else ""),
                    "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                    "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "ShippingContactName": shipping_addr.get("fullName", "") if shipping_addr else "",
                    "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "Si Number": safe_get(first_fulfillment, ['salesOrderLines', 0, 'siNumber']),
                    "Source System ID": safe_get(so, ['sourceSystemId']),
                    "Source System Status": safe_get(first_fulfillment, ['soStatus', 0,'sourceSystemStsCode']),
                    "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "System Qty": safe_get(first_fulfillment, ['systemQty']),
                    "Tax Regstrn Num": safe_get(first_fulfillment, ['address', 0, 'taxRegstrnNum']),
                    "Tie Number": safe_get(first_fulfillment, ['salesOrderLines', 0, 'soLineNum'])
                }

                # Process Work Orders
                if workorders:
                    for wo in workorders:
                        wo_row = {
                            "WO_ID": safe_get(wo, ['woId']),
                            "Dell Blanket PO Num": safe_get(wo, ['dellBlanketPoNum']),
                            "Ship To Facility": safe_get(wo, ['shipToFacility']),
                            "Is Last Leg": 'Y' if safe_get(wo, ['shipToFacility']) and 'CUST' in safe_get(wo, ['shipToFacility']).upper() else 'N',
                            "Ship From MCID": safe_get(wo, ['vendorSiteId']),
                            "WO OTM Enabled": safe_get(wo, ['isOtmEnabled']),
                            "WO Ship Mode": safe_get(wo, ['shipMode']),
                            "Is Multipack": safe_get(wo, ['woLines', 0, "ismultipack"]),
                            "Has Software": any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in safe_get(wo, ['woLines']) or []),
                            "Make WO Ack Date": next(
                                (
                                    dateFormation(status.get("statusDate"))
                                    for status in safe_get(wo, ['woStatusList']) or []
                                    if str(status.get("channelStatusCode")) == "3000" and safe_get(wo, ['woType']) == "MAKE"
                                ), ""
                            ),
                            "MCID Value": safe_get(wo, ['woShipInstr', 0, "mergeFacility"]) or safe_get(wo, ['woShipInstr', 0, "carrierHubCode"])
                        }
                        merged_row = {**base_row, **wo_row}
                        flat_list.append(merged_row)
                else:
                    flat_list.append(base_row)

        count_valid = len(ValidCount) if filtersValue else 0
        if not flat_list:
            return {"error": "No Data Found"}

        if format_type == "export":
            return [{"Count ": count_valid}, flat_list] if filtersValue else flat_list

        return flat_list

    except Exception as e:
        return {"error": str(e)}
