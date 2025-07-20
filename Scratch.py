for so_index, so_entry in enumerate(sales_orders):
    try:
        if not isinstance(so_entry, dict):
            print(f"[DEBUG] so_entry at index {so_index} is not a dict: {so_entry}")
            continue

        so_data = so_entry.get("data", {})
        get_soheaders = so_data.get("getSoheaderBySoids", [])
        get_salesorders = so_data.get("getBySalesorderids", [])

        if not get_soheaders:
            print(f"[WARN] Missing getSoheaderBySoids at index {so_index}")
            continue
        if not get_salesorders:
            print(f"[WARN] Missing getBySalesorderids at index {so_index}")
            continue

        soheader = get_soheaders[0]
        salesorder = get_salesorders[0]

        # Fulfillment
        fulfillment = {}
        sofulfillment = {}
        forderline = {}
        address = {}

        if so_index < len(fulfillments):
            fulfillment_data = fulfillments[so_index].get("data", {})
            fulfillment = fulfillment_data.get("getFulfillmentsById", {}) or {}
            sofulfillment = fulfillment_data.get("getFulfillmentsBysofulfillmentid", {}) or {}
            forderline = (fulfillment.get("salesOrderLines") or [{}])[0]
            address = (sofulfillment.get("address") or [{}])[0]
        else:
            print(f"[WARN] Missing fulfillment data for index {so_index}")

        if so_index < len(wo_ids):
            wo_data = wo_ids[so_index]
        else:
            wo_data = []
            print(f"[WARN] Missing wo_data for index {so_index}")

        # FOID
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

        # Flatten WO
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
        print(f"[ERROR] Formatting row {so_index}: {e}")
        import traceback
        traceback.print_exc()
        continue
