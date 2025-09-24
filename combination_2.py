 main function returns directly as "result"
            if "result" in data and isinstance(data["result"], list):
                return data["result"]

            return []

        for item in result_map:
            print("break point => 1 ")
            # exit()
            data = item.get("data") or item  # handle raw result if no "data" key
            if not data:
                continue

            sales_orders = extract_sales_order(data)
            if not sales_orders:
                continue
            print("break point => 2")
            exit()
            for so in sales_orders:
                fulfillments = listify(safe_get(so, ['fulfillments']))
                workorders = listify(safe_get(so, ['workOrders']))
                print("break point => 3")
                exit()
                # Track valid IDs
                if filtersValue:
                    sales_order_id = safe_get(so, ['salesOrderId'])
                    if region and region.upper() == safe_get(so, ['region'], "").upper():
                        ValidCount.append(sales_order_id)

                # Region filter
                if region and region.upper() != safe_get(so, ['region'], "").upper():
                    continue

                # Shipping / Billing addresses
                shipping_addr = pick_address_by_type(so, "SHIPPING")
                shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING") if fulfillments else {}
                billing_addr = pick_address_by_type(so, "BILLING")
                shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

                # LOB & Facility
                lob_list = list(filter(None, [safe_get(line, ['lob']) for line in safe_get(fulfillments, [0, 'salesOrderLines']) or []]))
                lob = ", ".join(lob_list)

                facility_list = list(filter(None, [safe_get(line, ['facility']) for line in safe_get(fulfillments, [0, 'salesOrderLines']) or []]))
                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f and f.strip()))

                # Status date function
                def get_status_date(code):
                    status_code = safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(fulfillments, [0, 'soStatus', 0, 'statusDate']))
                    return ""

                # ------------------- Sales Order + Fulfillment Row -------------------
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
                    "ShipToPhone": (listify(shipping_phone.get("phone", []))[0].get("phoneNumber", "") if shipping_phone and listify(shipping_phone.get("phone", [])) else ""),
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

                # ------------------- Work Orders Rows -------------------
                for WorkOrderData in workorders:
                    print("break point => 4")
                    exit()
                    WO_ID = safe_get(WorkOrderData, ['woId'])
                    DellBlanketPoNum = safe_get(WorkOrderData, ['dellBlanketPoNum'])
                    ship_to_facility = safe_get(WorkOrderData, ['shipToFacility'])
                    IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
                    ShipFromMcid = safe_get(WorkOrderData, ['vendorSiteId'])
                    WoOtmEnable = safe_get(WorkOrderData, ['isOtmEnabled'])
                    WoShipMode = safe_get(WorkOrderData, ['shipMode'])
                    wo_lines = safe_get(WorkOrderData, ['woLines'])
                    ismultipack = safe_get(wo_lines, [0,"ismultipack"]) if wo_lines else ""
                    has_software = any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in wo_lines)
                    MakeWoAckValue = next((dateFormation(status.get("statusDate")) for status in WorkOrderData.get("woStatusList", [])
                                           if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"), "")
                    McidValue = (
                        WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                        WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                    )

                    wo_row = {
                        "Sales Order ID": safe_get(so, ['salesOrderId']),
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

        count_valid = len(ValidCount)
        if not flat_list:
            return {"error": "No Data Found"}

        if format_type == "export":
            data = []
            if filtersValue:
                data.append({"Count ": count_valid})
            data.append(flat_list)
            ValidCount.clear()
            return data
        elif format_type == "grid":
            # Optional: prepare for table/grid output
            return flat_list

        return flat_list

    except Exception as e:
        return {"error": str(e)}
