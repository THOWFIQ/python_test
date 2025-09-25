for so in sales_orders:
    sales_order_id = safe_get(so, ['salesOrderId'])
    if region and region.upper() != safe_get(so, ['region'], "").upper():
        continue

    if filtersValue:
        ValidCount.append(sales_order_id)

    fulfillments = safe_get(so, ['fulfillments']) or []
    if isinstance(fulfillments, dict):
        fulfillments = [fulfillments]

    for fulfillment in fulfillments:
        # Decide fulfillmentId & foId properly
        if fulfillment.get("fulfillmentOrder"):
            fulfillment_id = safe_get(fulfillment, ['fulfillmentOrder', 0, 'fulfillmentId'])
            fo_id = safe_get(fulfillment, ['fulfillmentOrder', 0, 'foId'])
        else:
            fulfillment_id = safe_get(fulfillment, ['fulfillmentId'])
            fo_id = ""

        shipping_addr = pick_address_by_type(so, "SHIPPING")
        billing_addr = pick_address_by_type(so, "BILLING")
        shipping_phone = pick_address_by_type(fulfillment, "SHIPPING") if fulfillment else None
        shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

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

        row = {
            "Fulfillment ID": fulfillment_id,
            "FO ID": fo_id,
            "BUID": safe_get(so, ['buid']),
            "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
            "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
            "LOB": lob,
            "Sales Order ID": sales_order_id,
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
            "Order Type": safe_get(so, ['orderType']),
            # WO fields default
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

        # now attach WOs same as before
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
