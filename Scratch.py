def fileldValidation(filters, format_type, region):
    data_row_export = {}
    primary_in_filters = []
    secondary_in_filters = []

    for field in filters:
        if field in PRIMARY_FIELDS:
            primary_in_filters.append(field)
        elif field in SECONDARY_FIELDS:
            secondary_in_filters.append(field)

    if not primary_in_filters:
        return {
            "status": "error",
            "message": "At least one primary field is required in filters."
        }

    primary_filters = {key: filters[key] for key in primary_in_filters}
    secondary_filters = {key: filters[key] for key in secondary_in_filters}
    result_map = {}

    if 'Sales_Order_id' in primary_filters:
        so_ids = list(set(x.strip() for x in primary_filters['Sales_Order_id'].split(',') if x.strip()))
        salesorder_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_salesorder_fetch, soid) for soid in so_ids]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        salesorder_results.append(res)
                except Exception as e:
                    print(f"Error in SalesOrder ID fetch: {e}")
        result_map['Sales_Order_id'] = salesorder_results

    if 'foid' in primary_filters:
        foids = list(set(x.strip() for x in primary_filters['foid'].split(',') if x.strip()))
        foid_result = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_foid_fetch, foid) for foid in foids]
            for future in as_completed(futures):
                try:
                    foid_result.append(future.result())
                except Exception as e:
                    print(f"Error in FO ID fetch: {e}")
        result_map['foid'] = foid_result

    if 'wo_id' in primary_filters:
        woids = list(set(x.strip() for x in primary_filters['wo_id'].split(',') if x.strip()))
        woids_result = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_woid_fetch, woid) for woid in woids]
            for future in as_completed(futures):
                try:
                    woids_result.append(future.result())
                except Exception as e:
                    print(f"Error in WO ID fetch: {e}")

        result_map['wo_id'] = woids_result

    if 'Fullfillment Id' in primary_filters:
        ff_ids = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))
        fullfillment_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_fulfillment_fetch, fid) for fid in ff_ids]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        fullfillment_results.append(res)
                except Exception as e:
                    print(f"Error in Fullfillment Id fetch: {e}")
        result_map['Fullfillment Id'] = fullfillment_results

    formattingData = OutputFormat(result_map, format_type=format_type)

    return {
        "status": "success",
        "message": "Validation and fetch completed.",
        "result_summary": {key: f"{len(val)} response(s)" for key, val in result_map.items()},
        "data": formattingData
    }


def OutputFormat(result_map, format_type=None):
    flat_list = []

    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    wo_ids = result_map.get("wo_id", [])
    foid_data = result_map.get("foid", [])

    for so_index, so_entry in enumerate(sales_orders):
        try:
            so_data = so_entry.get("data", {})
            get_soheaders = so_data.get("getSoheaderBySoids", [])
            get_salesorders = so_data.get("getBySalesorderids", [])

            if not get_soheaders or not get_salesorders:
                print(f"Missing SO headers or sales orders at row {so_index}")
                continue

            soheader = get_soheaders[0]
            salesorder = get_salesorders[0]

            fulfillment_entry = fulfillments[so_index] if so_index < len(fulfillments) else {"data": {}}
            fulfillment_data = fulfillment_entry.get("data", {})

            fulfillment = fulfillment_data.get("getFulfillmentsById", {})
            sofulfillment = fulfillment_data.get("getFulfillmentsBysofulfillmentid", {})
            forderline = (fulfillment.get("salesOrderLines") or [{}])[0]
            address = (sofulfillment.get("address") or [{}])[0]

            wo_data = wo_ids[so_index] if so_index < len(wo_ids) else []

            data_row_export = {
                "BUID": soheader.get("buid"),
                "PP Date": soheader.get("ppDate"),
                "Sales Order Id": salesorder.get("salesOrderId"),
                "Fulfillment Id": fulfillment.get("fulfillmentId"),
                "Region Code": salesorder.get("region"),
                "FoId": (foid_data[0]["data"].get("getAllFulfillmentHeadersByFoId", [{}])[0].get("foId")
                         if foid_data and foid_data[0].get("data") else None),
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
                sn_numbers = wo.get("SN Number", [])
                wo_clean = {k: v for k, v in wo.items() if k != "SN Number"}

                if sn_numbers:
                    for sn in sn_numbers:
                        row = {**base, **wo_clean, "SN Number": sn}
                        flat_list.append(row)
                else:
                    row = {**base, **wo_clean, "SN Number": None}
                    flat_list.append(row)

        except Exception as e:
            print(f"Error formatting row {so_index} : {e}")

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
        return {"error": "Format type is not part of grid/export"}
