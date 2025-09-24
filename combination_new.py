def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    try:
        def extract_sales_order(data):
            if not data or not isinstance(data, dict):
                return None

            # Sales Orders
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

        graphql_details = result_map.get("graphql_details", [])

        for item_index, item in enumerate(graphql_details):
            if not isinstance(item, dict):
                print(f"Item index: {item_index} type: {type(item)}")
                print(f"Skipping non-dict item: {item}")
                continue

            data = item.get("data", {})
            if not data:
                continue

            # Extract Sales Orders and Work Orders
            sales_orders = extract_sales_order(data)
            workorders = data.get("getWorkOrderByWoIds", [])

            # Process Sales Orders
            if sales_orders:
                for so in sales_orders:
                    fulfillments = safe_get(so, [0, 'fulfillments']) or []
                    if isinstance(fulfillments, dict):
                        fulfillments = [fulfillments]

                    if filtersValue:
                        sales_order_id = safe_get(so, [0,'salesOrderId'])
                        if region and region.upper() == safe_get(so, [0,'region'], "").upper():
                            ValidCount.append(sales_order_id)

                    if region and region.upper() != safe_get(so, [0,'region'], "").upper():
                        continue

                    shipping_addr = pick_address_by_type(so[0], "SHIPPING")
                    billing_addr = pick_address_by_type(so[0], "BILLING")
                    shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING") if fulfillments else None
                    shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

                    # LOB and Facility
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

                    # Build row
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
                    flat_list.append(row)

            # Process Work Orders
            if workorders:
                for wo in workorders:
                    wo_row = {
                        "Sales Order ID": wo.get("woId"),
                        "WO_ID": wo.get("woId"),
                        "Vendor Site": wo.get("vendorSiteId"),
                        "Ship Mode": wo.get("shipMode"),
                        "WO Type": wo.get("woType"),
                        "Ship To Facility": wo.get("shipToFacility"),
                        "WO Status Code": safe_get(wo, ['woStatusList', 0, 'channelStatusCode'])
                    }
                    flat_list.append(wo_row)

        count_valid = len(ValidCount)
        if not flat_list:
            return {"error": "No Data Found"}

        if format_type == "export":
            data = [{"Count ": count_valid}, flat_list] if filtersValue else flat_list
            ValidCount.clear()
            return data

        elif format_type == "grid":
            desired_order = list(flat_list[0].keys())  # simple ordering
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
