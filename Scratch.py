def OutputFormat(resulData):
    result_map = json.loads(resulData)
    flat_list = []

    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    foids = result_map.get("foid", [])
    wo_ids = result_map.get("wo_id", [])

    for sales_order in sales_orders:
        so_data = sales_order["data"].get("getSoheaderBySoids", [])
        linkage_data = sales_order["data"].get("getBySalesorderids", [])

        for so in so_data:
            base_row = {}
            base_row["Sales Order Id"] = so.get("salesOrderId")
            base_row["Order Date"] = so.get("orderDate")
            base_row["BUID"] = so.get("buid")
            base_row["PP Date"] = so.get("ppDate")
            base_row["Region Code"] = so.get("region")

            # Fulfillment mapping
            for fulfillment in fulfillments:
                fdata = fulfillment["data"]
                ff_by_id = fdata.get("getFulfillmentsById", {})
                ff_by_soid = fdata.get("getFulfillmentsBysofulfillmentid", {})
                ff_header = fdata.get("getAllFulfillmentHeadersSoidFulfillmentid", {})
                fbom = fdata.get("getFbomBySoFulfillmentid", {})

                base_row["Fulfillment Id"] = ff_by_id.get("fulfillmentId")
                base_row["System Qty"] = ff_by_id.get("systemQty")
                base_row["Ship By Date"] = ff_by_id.get("shipByDate")
                base_row["LOB"] = ff_by_id.get("salesOrderLines", [{}])[0].get("lob") if ff_by_id.get("salesOrderLines") else None
                base_row["Ship Code"] = ff_by_soid.get("shipCode")
                base_row["Must Arrive By Date"] = ff_by_soid.get("mustArriveByDate")
                base_row["Update Date"] = ff_by_soid.get("updateDate")
                base_row["Manifest Date"] = ff_by_soid.get("manifestDate")
                base_row["Revised Delivery Date"] = ff_by_soid.get("revisedDeliveryDate")
                base_row["Delivery City"] = ff_by_soid.get("deliveryCity")
                base_row["OIC Id"] = ff_by_soid.get("oicId")

                address = ff_by_soid.get("address", [{}])[0]
                base_row["Tax Regstrn Num"] = address.get("taxRegstrnNum")
                base_row["Address Line1"] = address.get("addressLine1")
                base_row["Postal Code"] = address.get("postalCode")
                base_row["State Code"] = address.get("stateCode")
                base_row["City Code"] = address.get("cityCode")
                base_row["Customer Num"] = address.get("customerNum")
                base_row["Customer Name Ext"] = address.get("customerNameExt")
                base_row["Country"] = address.get("country")
                base_row["Create Date"] = address.get("createDate")

                # DirectShip
                base_row["IsDirect Ship"] = ff_header.get("isDirectShip")
                base_row["Source System Id"] = ff_header.get("sourceSystemId")
                base_row["SSC"] = ff_header.get("ssc")

                # Foid
                for fo in foids:
                    fo_data = fo["data"].get("getAllFulfillmentHeadersByFoId", {})
                    base_row["FoId"] = fo_data.get("foId")
                    base_row["Ship From Facility"] = fo_data.get("shipFromFacility")
                    base_row["Ship To Facility"] = fo_data.get("shipToFacility")

                # WO (Multiple, will loop and create new row per SN)
                for wo in wo_ids:
                    wo_data = wo
                    flat_row = base_row.copy()
                    flat_row["Vendor Work Order Num"] = wo_data.get("Vendor Work Order Num")
                    flat_row["Channel Status Code"] = wo_data.get("Channel Status Code")
                    flat_row["Ismultipack"] = wo_data.get("Ismultipack")
                    flat_row["Ship Mode"] = wo_data.get("Ship Mode")
                    flat_row["Is Otm Enabled"] = wo_data.get("Is Otm Enabled")

                    sn_list = wo_data.get("SN Number", [])
                    if sn_list:
                        for sn in sn_list:
                            sn_row = flat_row.copy()
                            sn_row["SN Number"] = sn
                            flat_list.append(sn_row)
                    else:
                        flat_row["SN Number"] = None
                        flat_list.append(flat_row)

    print(json.dumps(flat_list, indent=2))
    return flat_list
