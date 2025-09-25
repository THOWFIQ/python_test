def OutputFormat(result_map, format_type=None, region=None):
    try:
        output = []

        # Count only once at the start
        output.append({"Count ": len(result_map)})

        rows = []
        for item in result_map:
            try:
                sales_order = item.get("salesOrder", {})
                fulfillment = safe_get(item, ["fulfillment", 0], {})
                billing_addr = item.get("billingAddress", {})
                shipping_addr = item.get("shippingAddress", {})
                shipping_contact = safe_get(item, ["shippingContact", "name"], "")

                # Work Orders list
                workorders = item.get("getWorkOrderByWoIds", [])

                # Base sales order data
                base_row = {
                    "Agreement ID": sales_order.get("agreementId", ""),
                    "Amount": sales_order.get("amount", ""),
                    "BUID": sales_order.get("buid", ""),
                    "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                    "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "Country": shipping_addr.get("countryCode", "") if shipping_addr else "",
                    "Currency Code": sales_order.get("currencyCode", ""),
                    "Customer Name Ext": sales_order.get("customerNameExt", ""),
                    "Customer Num": sales_order.get("customerNumber", ""),
                    "Customer Po Number": sales_order.get("customerPoNumber", ""),
                    "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "DOMS Status": fulfillment.get("status", ""),
                    "Delivery City": shipping_addr.get("cityName", ""),
                    "Dp ID": sales_order.get("dpId", ""),
                    "FO ID": fulfillment.get("foId", ""),
                    "Facility": fulfillment.get("facility", ""),
                    "Fulfillment ID": fulfillment.get("fulfillmentId", ""),
                    "Fulfillment Status": fulfillment.get("fulfillmentStatus", ""),
                    "IP Date": fulfillment.get("ipDate", ""),
                    "InstallInstruction2": fulfillment.get("installInstruction2", ""),
                    "LOB": sales_order.get("lob", ""),
                    "Location Number": fulfillment.get("locationNumber", ""),
                    "MN Date": fulfillment.get("mnDate", ""),
                    "Manifest Date": fulfillment.get("manifestDate", ""),
                    "Merge Type": fulfillment.get("mergeType", ""),
                    "Must Arrive By Date": fulfillment.get("mustArriveByDate", ""),
                    "OFS Status": fulfillment.get("ofsStatus", ""),
                    "OFS Status Code": fulfillment.get("ofsStatusCode", ""),
                    "OIC ID": sales_order.get("oicId", ""),
                    "Order Age": sales_order.get("orderAge", ""),
                    "Order Amount usd": sales_order.get("orderAmountUsd", ""),
                    "Order Date": sales_order.get("orderDate", ""),
                    "Order Type": sales_order.get("orderType", ""),
                    "PP Date": sales_order.get("ppDate", ""),
                    "Payment Term Code": sales_order.get("paymentTermCode", ""),
                    "Rate Usd Transactional": sales_order.get("rateUsdTransactional", ""),
                    "Reassigned IP Date": sales_order.get("reassignedIpDate", ""),
                    "Region Code": sales_order.get("region", ""),
                    "Req Ship Code": fulfillment.get("reqShipCode", ""),
                    "Revised Delivery Date": fulfillment.get("revisedDeliveryDate", ""),
                    "SC Date": fulfillment.get("scDate", ""),
                    "Sales Order ID": sales_order.get("salesOrderId", ""),
                    "Sales Rep Name": sales_order.get("salesRepName", ""),
                    "Ship By Date": fulfillment.get("shipByDate", ""),
                    "Ship Code": fulfillment.get("shipCode", ""),
                    "ShipToAddress1": shipping_addr.get("address1", ""),
                    "ShipToAddress2": shipping_addr.get("address2", ""),
                    "ShipToCompany": shipping_addr.get("companyName", ""),
                    "ShipToPhone": shipping_addr.get("phoneNumber", ""),
                    "ShipToPostal": shipping_addr.get("postalCode", ""),
                    "Shipping Country": shipping_addr.get("countryCode", ""),
                    "ShippingCityCode": shipping_addr.get("cityCode", ""),
                    "ShippingContactName": shipping_contact,
                    "ShippingCustName": shipping_addr.get("companyName", ""),
                    "ShippingStateCode": shipping_addr.get("stateCode", ""),
                    "Si Number": fulfillment.get("siNumber", ""),
                    "Source System ID": sales_order.get("sourceSystemId", ""),
                    "Source System Status": sales_order.get("sourceSystemStatus", ""),
                    "State Code": shipping_addr.get("stateCode", ""),
                    "System Qty": fulfillment.get("systemQty", ""),
                    "Tax Regstrn Num": sales_order.get("taxRegistrationNum", ""),
                    "Tie Number": fulfillment.get("tieNumber", "")
                }

                # If no WOs -> still add one record with just sales order info
                if not workorders:
                    rows.append(base_row)
                else:
                    # For each WO, merge with base sales order data
                    for wo in workorders:
                        wo_row = base_row.copy()
                        wo_row.update({
                            "Dell Blanket PO Num": wo.get("dellBlanketPoNum", ""),
                            "Has Software": wo.get("hasSoftware", False),
                            "Is Last Leg": wo.get("isLastLeg", ""),
                            "Is Multipack": wo.get("isMultipack", ""),
                            "MCID Value": wo.get("mcidValue", ""),
                            "Make WO Ack Date": wo.get("makeWoAckDate", ""),
                            "Ship From MCID": wo.get("shipFromMcid", ""),
                            "Ship To Facility": wo.get("shipToFacility", ""),
                            "WO OTM Enabled": wo.get("woOtmEnabled", ""),
                            "WO Ship Mode": wo.get("woShipMode", ""),
                            "WO_ID": wo.get("woId", "")
                        })
                        rows.append(wo_row)

            except Exception as e:
                print("Error processing item:", str(e))
                continue

        output.append(rows)
        return output

    except Exception as e:
        print("Error in OutputFormat:", str(e))
        return []
