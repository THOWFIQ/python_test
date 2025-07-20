def OutputFormat(result_map, format_type=None):
    import json
    flat_list = []

    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    wo_ids = result_map.get("wo_id", [])
    foid_data = result_map.get("foid", [])

    for so_index, so_entry in enumerate(sales_orders):
        try:
            if not isinstance(so_entry, dict):
                print(f"[WARN] sales_orders[{so_index}] is not a dict.")
                continue

            so_data = so_entry.get("data", {})
            get_soheaders = so_data.get("getSoheaderBySoids", [])
            get_salesorders = so_data.get("getBySalesorderids", [])

            if not get_soheaders or not get_salesorders:
                print(f"[WARN] Missing SO headers or sales orders at row {so_index}")
                continue

            soheader = get_soheaders[0] if isinstance(get_soheaders, list) else {}
            salesorder = get_salesorders[0] if isinstance(get_salesorders, list) else {}

            fulfillment = {}
            sofulfillment = {}
            forderline = {}
            address = {}

            if so_index < len(fulfillments):
                fulfillment_entry = fulfillments[so_index]
                if isinstance(fulfillment_entry, dict):
                    fulfillment_data = fulfillment_entry.get("data", {})
                    f_raw = fulfillment_data.get("getFulfillmentsById")
                    s_raw = fulfillment_data.get("getFulfillmentsBysofulfillmentid")

                    # Handle if 'getFulfillmentsById' is a list
                    if isinstance(f_raw, list):
                        fulfillment = f_raw[0] if f_raw else {}
                    elif isinstance(f_raw, dict):
                        fulfillment = f_raw

                    if isinstance(s_raw, list):
                        sofulfillment = s_raw[0] if s_raw else {}
                    elif isinstance(s_raw, dict):
                        sofulfillment = s_raw

                    forderline = (fulfillment.get("salesOrderLines") or [{}])[0]
                    address = (sofulfillment.get("address") or [{}])[0]

            if so_index < len(wo_ids):
                wo_data = wo_ids[so_index]
                if isinstance(wo_data, str):
                    wo_data = json.loads(wo_data)
            else:
                wo_data = []

            if not isinstance(wo_data, list):
                wo_data = [wo_data]

            foid_entry = None
            if foid_data and isinstance(foid_data[0], dict):
                foid_entry = foid_data[0].get("data", {}).get("getAllFulfillmentHeadersByFoId", [{}])[0]

            data_row_export = {
                "BUID": soheader.get("buid"),
                "PP Date": soheader.get("ppDate"),
                "Sales Order Id": salesorder.get("salesOrderId"),
                "Fulfillment Id": fulfillment.get("fulfillmentId"),
                "Region Code": salesorder.get("region"),
                "FoId": foid_entry.get("foId") if foid_entry else None,
                "System Qty": fulfillment.get("systemQty"),
                "Ship By Date": fulfillment.get("shipByDate"),
                "LOB": forderline.get("lob"),
                "Ship From Facility": forderline.get("shipFromFacility"),
                "Ship To Facility": forderline.get("shipToFacility"),
                "Tax Regstrn Num": address.get("taxRegstrnNum"),
                "Address Line1": address.get("addressLine1"),
                "Postal Code": address.get("postalCode"),
                "State Code": address.get("stateCode"),
                "City Code": address.get("cityCode"),
                "Customer Num": address.get("customerNum"),
                "Customer Name Ext": address.get("customerNameExt"),
                "Country": address.get("country"),
                "Create Date": address.get("createDate"),
                "Ship Code": sofulfillment.get("shipCode"),
                "Must Arrive By Date": sofulfillment.get("mustArriveByDate"),
                "Update Date": sofulfillment.get("updateDate"),
                "Merge Type": sofulfillment.get("mergeType"),
                "Manifest Date": sofulfillment.get("manifestDate"),
                "Revised Delivery Date": sofulfillment.get("revisedDeliveryDate"),
                "Delivery City": sofulfillment.get("deliveryCity"),
                "Source System Id": sofulfillment.get("sourceSystemId"),
                "IsDirect Ship": sofulfillment.get("isDirectShip"),
                "SSC": sofulfillment.get("ssc"),
                "OIC Id": sofulfillment.get("oicId"),
                "Order Date": soheader.get("orderDate"),
                "wo_ids": wo_data,
            }

            base = {k: v for k, v in data_row_export.items() if k != "wo_ids"}

            for wo in wo_data:
                if isinstance(wo, str):
                    try:
                        wo = json.loads(wo)
                    except:
                        continue

                if not isinstance(wo, dict):
                    continue

                sn_numbers = wo.get("SN Number", [])
                wo_clean = {k: v for k, v in wo.items() if k != "SN Number"}

                if sn_numbers and isinstance(sn_numbers, list):
                    for sn in sn_numbers:
                        row = {**base, **wo_clean, "SN Number": sn}
                        flat_list.append(row)
                else:
                    row = {**base, **wo_clean, "SN Number": None}
                    flat_list.append(row)

        except Exception as e:
            print(f"[ERROR] formatting row {so_index}: {e}")
            import traceback
            traceback.print_exc()
            continue

    if format_type == "export":
        return flat_list

    elif format_type == "grid":
        desired_order = [
            'BUID','PP Date','Sales Order Id','Fulfillment Id','Region Code','FoId','System Qty','Ship By Date',
            'LOB','Ship From Facility','Ship To Facility','Tax Regstrn Num','Address Line1','Postal Code','State Code',
            'City Code','Customer Num','Customer Name Ext','Country','Create Date','Ship Code','Must Arrive By Date',
            'Update Date','Merge Type','Manifest Date','Revised Delivery Date','Delivery City','Source System Id','IsDirect Ship',
            'SSC','Vendor Work Order Num','Channel Status Code','Ismultipack','Ship Mode','Is Otm Enabled',
            'SN Number','OIC Id', 'Order Date'
        ]

        rows = []
        for item in flat_list:
            row = {
                "columns": [{"value": item.get(key, "")} for key in desired_order]
            }
            rows.append(row)

        return rows

    else:
        return {"error": "Format type must be either 'grid' or 'export'"}
