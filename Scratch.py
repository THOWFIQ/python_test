def OutputFormat(result_map, format_type='export'):
    import json

    # Parse string to dict if it's JSON string
    if isinstance(result_map, str):
        try:
            result_map = json.loads(result_map)
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON string: {e}")
            return []

    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    foid_data = result_map.get("foid", [])
    wo_data = result_map.get("wo_id", [])

    flat_list = []

    for so_index, so_entry in enumerate(sales_orders):
        try:
            soheader = so_entry["data"].get("getSoheaderBySoids", [{}])[0]
            salesorder = so_entry["data"].get("getBySalesorderids", [{}])[0]

            fulfillment = fulfillments[so_index]["data"].get("getFulfillmentsById", {})
            sofulfillment = fulfillments[so_index]["data"].get("getFulfillmentsBysofulfillmentid", {})
            forderline = fulfillment.get("salesOrderLines", [{}])[0]
            address = sofulfillment.get("address", [{}])[0]

            data_row_export = {
                "BUID": soheader.get("buid"),
                "PP Date": soheader.get("ppDate"),
                "Sales Order Id": salesorder.get("salesOrderId"),
                "Fulfillment Id": fulfillment.get("fulfillmentId"),
                "Region Code": salesorder.get("region"),
                "FoId": foid_data[0]["data"].get("getAllFulfillmentHeadersByFoId", [{}])[0].get("foId"),
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
                "wo_ids": wo_data
            }

            base = {k: v for k, v in data_row_export.items() if k != "wo_ids"}

            for wo in wo_data:
                sn_list = wo.get("SN Number", [])
                wo_flat = {k: v[0] if isinstance(v, tuple) else v for k, v in wo.items() if k != "SN Number"}

                if sn_list:
                    for sn in sn_list:
                        flat_entry = {**base, **wo_flat, "SN Number": sn}
                        flat_list.append(flat_entry)
                else:
                    flat_entry = {**base, **wo_flat, "SN Number": None}
                    flat_list.append(flat_entry)

        except Exception as e:
            print(f"Error formatting row {so_index}: {e}")

    if format_type == "export":
        return flat_list

    elif format_type == "grid":
        desired_order = [
            'BUID', 'PP Date', 'Sales Order Id', 'Fulfillment Id', 'Region Code', 'FoId', 'System Qty', 'Ship By Date',
            'LOB', 'Ship From Facility', 'Ship To Facility', 'Tax Regstrn Num', 'Address Line1', 'Postal Code',
            'State Code', 'City Code', 'Customer Num', 'Customer Name Ext', 'Country', 'Create Date', 'Ship Code',
            'Must Arrive By Date', 'Update Date', 'Merge Type', 'Manifest Date', 'Revised Delivery Date',
            'Delivery City', 'Source System Id', 'IsDirect Ship', 'SSC', 'Vendor Work Order Num', 'Channel Status Code',
            'Ismultipack', 'Ship Mode', 'Is Otm Enabled', 'SN Number', 'OIC Id', 'Order Date'
        ]

        rows = []
        for item in flat_list:
            reordered_values = [item.get(key, "") for key in desired_order]
            row = {
                "columns": [{"value": val} for val in reordered_values]
            }
            rows.append(row)

        return rows

    else:
        return {"error": "Format type is not part of grid/export"}
