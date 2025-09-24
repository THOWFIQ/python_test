def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    try:
        flat_list = []
        ValidCount = []

        for item in result_map:
            data = item.get("data")
            if not data:
                continue

            # --- Sales Orders + Fulfillments ---
            soids_data = data.get("getSalesOrderBySoids")
            if soids_data and soids_data.get("salesOrders"):
                for so in soids_data["salesOrders"]:
                    fulfillments = safe_get(so, ["fulfillments"]) or []
                    if isinstance(fulfillments, dict):
                        fulfillments = [fulfillments]

                    shipping_addr = pick_address_by_type(so, "SHIPPING")
                    billing_addr = pick_address_by_type(so, "BILLING")
                    shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING") if fulfillments else {}
                    shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

                    # Collect LOB and Facility
                    lob_list = list(filter(
                        None,
                        [safe_get(line, ["lob"]) for line in safe_get(fulfillments, [0, "salesOrderLines"]) or []]
                    ))
                    lob = ", ".join(lob_list)

                    facility_list = list(filter(
                        None,
                        [safe_get(line, ["facility"]) for line in safe_get(fulfillments, [0, "salesOrderLines"]) or []]
                    ))
                    facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f and f.strip()))

                    def get_status_date(code):
                        status_code = safe_get(fulfillments, [0, "soStatus", 0, "sourceSystemStsCode"])
                        if status_code == code:
                            return dateFormation(safe_get(fulfillments, [0, "soStatus", 0, "statusDate"]))
                        return ""

                    row = {
                        "Record Type": "SalesOrder",
                        "Sales Order ID": safe_get(so, ["salesOrderId"]),
                        "Fulfillment ID": safe_get(fulfillments, [0, "fulfillmentId"]),
                        "BUID": safe_get(so, ["buid"]),
                        "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                        "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                        "LOB": lob,
                        "Facility": facility,
                        "Amount": safe_get(so, ["totalPrice"]),
                        "Currency": safe_get(so, ["currency"]),
                        "Region": safe_get(so, ["region"]),
                        "Order Date": dateFormation(safe_get(so, ["orderDate"])),
                        "PP Date": get_status_date("PP"),
                        "IP Date": get_status_date("IP"),
                        "MN Date": get_status_date("MN"),
                        "SC Date": get_status_date("SC"),
                    }
                    flat_list.append(row)

            # --- Work Orders ---
            woids_data = data.get("getWorkOrderByWoIds")
            if woids_data and isinstance(woids_data, list):
                for wo in woids_data:
                    wo_lines = wo.get("woLines", [])
                    has_software = any(line.get("woLineType") == "SOFTWARE" for line in wo_lines)
                    ismultipack = safe_get(wo_lines, [0, "ismultipack"])

                    make_ack_value = next(
                        (dateFormation(status.get("statusDate"))
                         for status in wo.get("woStatusList", [])
                         if str(status.get("channelStatusCode")) == "3000" and wo.get("woType") == "MAKE"),
                        ""
                    )

                    row = {
                        "Record Type": "WorkOrder",
                        "WO ID": safe_get(wo, ["woId"]),
                        "WO Type": safe_get(wo, ["woType"]),
                        "Channel": safe_get(wo, ["channel"]),
                        "Vendor Site": safe_get(wo, ["vendorSiteId"]),
                        "Ship To Facility": safe_get(wo, ["shipToFacility"]),
                        "Ship Mode": safe_get(wo, ["shipMode"]),
                        "Is Multipack": ismultipack,
                        "Has Software": "Y" if has_software else "N",
                        "Dell Blanket PO": safe_get(wo, ["dellBlanketPoNum"]),
                        "WO Ack Date": make_ack_value,
                    }
                    flat_list.append(row)

        if not flat_list:
            return {"error": "No Data Found"}

        # --- Export or Grid Formatting ---
        if format_type == "export":
            return flat_list
        elif format_type == "grid":
            rows = []
            for item in flat_list:
                row = {"columns": [{"value": v if v is not None else ""} for v in item.values()]}
                rows.append(row)
            return rows

        return flat_list

    except Exception as e:
        return {"error": str(e)}


woids_data = data.get("data", {}).get("getWorkOrderByWoIds")

if not woids_data:
    print("⚠️ No work orders found in response:", json.dumps(data, indent=2))
else:
    print("✅ Work orders:", json.dumps(woids_data, indent=2))

