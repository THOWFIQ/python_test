def OutputFormat(result_map):
    flat_list = []

    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    foids = result_map.get("foid", [])
    wo_ids = result_map.get("wo_id", [])

    for idx, so_entry in enumerate(sales_orders):
        try:
            soheader = so_entry["data"].get("getSoheaderBySoids", [{}])[0]
            salesOrder = so_entry["data"].get("getBySalesorderids", [{}])[0]

            fulfillment_data = fulfillments[idx]["data"] if idx < len(fulfillments) else {}
            fo_data = foids[idx]["data"] if idx < len(foids) else {}
            wo_data_list = wo_ids[idx] if idx < len(wo_ids) else []

            ff_by_id = fulfillment_data.get("getFulfillmentsById", {})
            ff_by_so = fulfillment_data.get("getFulfillmentsBysofulfillmentid", {})
            address_list = ff_by_so.get("address", [])

            address = address_list[0] if address_list else {}

            # Shared fields (base row)
            base_row = {
                "BUID": soheader.get("buid"),
                "PP Date": soheader.get("ppDate"),
                "Sales Order Id": salesOrder.get("salesOrderId"),
                "Fulfillment Id": ff_by_id.get("fulfillmentId"),
                "Region Code": salesOrder.get("region"),
                "FoId": fo_data.get("getAllFulfillmentHeadersByFoId", [{}])[0].get("foId"),
                "System Qty": ff_by_id.get("systemQty"),
                "Ship By Date": ff_by_id.get("shipByDate"),
                "LOB": ff_by_id.get("salesOrderLines", [{}])[0].get("lob"),
                "Ship From Facility": ff_by_id.get("forderlines", [{}])[0].get("shipFromFacility"),
                "Ship To Facility": ff_by_id.get("forderlines", [{}])[0].get("shipToFacility"),
                "Tax Regstrn Num": address.get("taxRegstrnNum"),
                "Address Line1": address.get("addressLine1"),
                "Postal Code": address.get("postalCode"),
                "State Code": address.get("stateCode"),
                "City Code": address.get("cityCode"),
                "Customer Num": address.get("customerNum"),
                "Customer Name Ext": address.get("customerNameExt"),
                "Country": address.get("country"),
                "Create Date": address.get("createDate"),
                "Ship Code": ff_by_so.get("shipCode"),
                "Must Arrive By Date": ff_by_so.get("mustArriveByDate"),
                "Update Date": ff_by_so.get("updateDate"),
                "Merge Type": ff_by_so.get("mergeType"),
                "Manifest Date": ff_by_so.get("manifestDate"),
                "Revised Delivery Date": ff_by_so.get("revisedDeliveryDate"),
                "Delivery City": ff_by_so.get("deliveryCity"),
                "Source System Id": ff_by_so.get("sourceSystemId"),
                "IsDirect Ship": ff_by_so.get("isDirectShip"),
                "SSC": ff_by_so.get("ssc"),
                "OIC Id": ff_by_so.get("oicId"),
                "Order Date": soheader.get("orderDate")
            }

            # Loop through work orders
            for wo in wo_data_list:
                flat_wo = {k: v[0] if isinstance(v, tuple) else v for k, v in wo.items() if k != "SN Number"}
                sn_list = wo.get("SN Number", [])

                if sn_list:
                    for sn in sn_list:
                        row = {**base_row, **flat_wo, "SN Number": sn}
                        flat_list.append(row)
                else:
                    row = {**base_row, **flat_wo, "SN Number": None}
                    flat_list.append(row)

        except Exception as e:
            print(f"Error formatting entry {idx}: {e}")
            continue

    print(json.dumps(flat_list, indent=2))
    return flat_list
