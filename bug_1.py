def newOutputFormat_withSequence(result_map, SequenceValue, format_type=None, region=None, filtersValue=None):
    try:
        flat_list = []

        for seq in SequenceValue:
            salesOrderId = seq.get("salesOrder", {}).get("salesOrderId")
            woId = seq.get("workOrder", {}).get("woId")
            fulfillmentId = seq.get("fulfillment", {}).get("fulfillmentId")

            # Find the SO data
            so = next(
                (so for item in result_map
                 for so in item.get("getSalesOrderByFfids", {}).get("salesOrders", [])
                 if safe_get(so, ['salesOrderId']) == salesOrderId),
                None
            )

            # Find the WO data
            wo_data = next(
                (wo for item in result_map
                 for wo in item.get("getWorkOrderByWoIds", [])
                 if safe_get(wo, ['woId']) == woId),
                None
            )

            if not so:
                continue  # skip if SO not found

            # Get fulfillments from SO
            fulfillments = safe_get(so, ['fulfillments']) or []
            if isinstance(fulfillments, dict):
                fulfillments = [fulfillments]

            fulfillment = next(
                (f for f in fulfillments if safe_get(f, ['fulfillmentId']) == fulfillmentId),
                {}
            )

            # Shipping and billing
            shipping_addr = pick_address_by_type(so, "SHIPPING")
            billing_addr = pick_address_by_type(so, "BILLING")
            shipping_phone = pick_address_by_type(fulfillment, "SHIPPING") if fulfillment else None
            shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

            # LOB and Facility
            lob_list = list(filter(
                lambda lob: lob and lob.strip() != "",
                map(lambda line: safe_get(line, ['lob']), safe_get(fulfillment, ['salesOrderLines']) or [])
            ))
            lob = ", ".join(lob_list)

            facility_list = list(filter(
                lambda f: f and f.strip() != "",
                map(lambda line: safe_get(line, ['facility']), safe_get(fulfillment, ['salesOrderLines']) or [])
            ))
            facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f))

            def get_status_date(code):
                status_code = safe_get(fulfillment, ['soStatus', 0, 'sourceSystemStsCode'])
                if status_code == code:
                    return dateFormation(safe_get(fulfillment, ['soStatus', 0, 'statusDate']))
                return ""

            # WO row
            wo_row = {}
            if wo_data:
                wo_row = {
                    "WO_ID": safe_get(wo_data, ['woId']),
                    "Dell Blanket PO Num": safe_get(wo_data, ['dellBlanketPoNum']),
                    "Ship To Facility": safe_get(wo_data, ['shipToFacility']),
                    "Is Last Leg": 'Y' if safe_get(wo_data, ['shipToFacility']) else 'N',
                    "Ship From MCID": safe_get(wo_data, ['vendorSiteId']),
                    "WO OTM Enabled": safe_get(wo_data, ['isOtmEnabled']),
                    "WO Ship Mode": safe_get(wo_data, ['shipMode']),
                    "Is Multipack": safe_get(wo_data, ['woLines', 0, 'ismultipack']),
                    "Has Software": any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in safe_get(wo_data, ['woLines']) or []),
                    "Make WO Ack Date": next(
                        (dateFormation(status.get("statusDate"))
                         for status in safe_get(wo_data, ['woStatusList']) or []
                         if str(status.get("channelStatusCode")) == "3000" and safe_get(wo_data, ['woType']) == "MAKE"),
                        ""
                    ),
                    "MCID Value": (
                        safe_get(wo_data, ['woShipInstr', 0, "mergeFacility"]) or
                        safe_get(wo_data, ['woShipInstr', 0, "carrierHubCode"])
                    )
                }

            # Build the final row
            row = {
                "Fulfillment ID": safe_get(fulfillment, ['fulfillmentId']),
                "BUID": safe_get(so, ['buid']),
                "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                "LOB": lob,
                "Sales Order ID": salesOrderId,
                "Agreement ID": safe_get(so, ['agreementId']),
                "Amount": safe_get(so, ['totalPrice']),
                "Currency Code": safe_get(so, ['currency']),
                "Customer Po Number": safe_get(so, ['poNumber']),
                "Delivery City": safe_get(fulfillment, ['deliveryCity']),
                "DOMS Status": safe_get(fulfillment, ['soStatus', 0, 'sourceSystemStsCode']),
                "Dp ID": safe_get(so, ['dpid']),
                "Fulfillment Status": safe_get(fulfillment, ['soStatus', 0, 'fulfillmentStsCode']),
                "Merge Type": safe_get(fulfillment, ['mergeType']),
                "InstallInstruction2": get_install_instruction2_id(so),
                "PP Date": get_status_date("PP"),
                "IP Date": get_status_date("IP"),
                "MN Date": get_status_date("MN"),
                "SC Date": get_status_date("SC"),
                "Location Number": safe_get(so, ['locationNum']),
                "OFS Status Code": safe_get(fulfillment, ['soStatus', 0, 'sourceSystemStsCode']),
                "OFS Status": safe_get(fulfillment, ['soStatus', 0, 'fulfillmentStsCode']),
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
                "Source System Status": safe_get(fulfillment, ['soStatus', 0,'sourceSystemStsCode']),
                "Tie Number": safe_get(fulfillment, ['salesOrderLines', 0, 'soLineNum']),
                "Si Number": safe_get(fulfillment, ['salesOrderLines', 0, 'siNumber']),
                "Req Ship Code": safe_get(fulfillment, ['shipCode']),
                "Reassigned IP Date": safe_get(fulfillment, ['soStatus', 0, 'sourceSystemStsCode']),
                "Payment Term Code": safe_get(fulfillment, ['paymentTerm']),
                "Region Code": safe_get(so, ['region']),
                "FO ID": safe_get(fulfillment, ['fulfillmentOrder', 0, 'foId']),
                "System Qty": safe_get(fulfillment, ['systemQty']),
                "Ship By Date": safe_get(fulfillment, ['shipByDate']),
                "Facility": facility,
                "Tax Regstrn Num": safe_get(fulfillment, ['address', 0, 'taxRegstrnNum']),
                "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
                "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
                "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
                "Country": shipping_addr.get("country", "") if shipping_addr else "",
                "Ship Code": safe_get(fulfillment, ['shipCode']),
                "Must Arrive By Date": dateFormation(safe_get(fulfillment, ['mustArriveByDate'])),
                "Manifest Date": dateFormation(safe_get(fulfillment, ['manifestDate'])),
                "Revised Delivery Date": dateFormation(safe_get(fulfillment, ['revisedDeliveryDate'])),
                "Source System ID": safe_get(so, ['sourceSystemId']),
                "OIC ID": safe_get(fulfillment, ['oicId']),
                "Order Date": dateFormation(safe_get(so, ['orderDate'])),
                "Order Type": dateFormation(safe_get(so, ['orderType']))
            }

            # Merge WO fields if exists
            if wo_row:
                row.update(wo_row)

            flat_list.append(row)

        # Handle export/grid
        count_valid = len(flat_list)
        if not flat_list:
            return {"error": "No Data Found"}

        if format_type == "export":
            data = [{"Count ": count_valid}, flat_list] if filtersValue else flat_list
            ValidCount.clear()
            return data

        elif format_type == "grid":
            desired_order = list(flat_list[0].keys())
            rows = [{"columns": [{"value": item.get(k, "")} for k in desired_order]} for item in flat_list]
            table_grid_output = tablestructural(rows, region) if rows else []
            if filtersValue:
                table_grid_output["Count"] = count_valid
            ValidCount.clear()
            return table_grid_output

        return flat_list

    except Exception as e:
        return {"error": str(e)}
