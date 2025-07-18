def fetch_and_clean(salesorder_id, region):
    combined_data = {'data': {}}

    # Region-specific endpoints
    FID = get_path(region, "FID", CONFIG)
    FOID = get_path(region, "FOID", CONFIG)
    SOPATH = get_path(region, "SOPATH", CONFIG)
    WOID = get_path(region, "WOID", CONFIG)
    FFBOM = get_path(region, "FFBOM", CONFIG)

    soi = {"salesorderIds": [salesorder_id]}

    # 1. SO HEADER
    so_header_resp = post_api(SOPATH, fetch_soaorder_query(), soi)
    so_header = so_header_resp.get('data', {}).get('getSoheaderBySoids', [])
    if not so_header:
        raise ValueError(f"No SO Header found for {salesorder_id}")
    combined_data['data']['getSoheaderBySoids'] = so_header

    # 2. SALES ORDER
    salesorder_resp = post_api(FID, fetch_salesorder_query(salesorder_id))
    get_by_so = salesorder_resp.get('data', {}).get('getBySalesorderids')
    if not get_by_so or not get_by_so.get('result'):
        raise ValueError(f"No Sales Order data for {salesorder_id}")
    result = get_by_so['result'][0]
    combined_data['data']['getBySalesorderids'] = get_by_so

    # 3. WORK ORDERS
    for wo in result.get("workOrders", []):
        wo_id = wo.get("woId")
        if not wo_id:
            continue

        # Work Order detail
        wo_detail_resp = post_api(WOID, fetch_workOrderId_query(wo_id))
        wo_detail = wo_detail_resp.get("data", {}).get("getWorkOrderById", [])
        if not wo_detail:
            continue
        wo_enriched = wo_detail[0]

        # ASN Numbers
        sn_resp = post_api(FID, fetch_getByWorkorderids_query(wo_id))
        sn_data = sn_resp.get("data", {}).get("getByWorkorderids", {}).get("result", [])
        sn_list = []
        if sn_data and "asnNumbers" in sn_data[0]:
            sn_list = [sn.get("snNumber", "") for sn in sn_data[0]["asnNumbers"] if sn.get("snNumber")]

        wo.update({
            "Vendor Work Order Num": wo_enriched.get("woId", ""),
            "Channel Status Code": wo_enriched.get("channelStatusCode", ""),
            "Ismultipack": (wo_enriched.get("woLines") or [{}])[0].get("ismultipack", ""),
            "Ship Mode": wo_enriched.get("shipMode", ""),
            "Is Otm Enabled": wo_enriched.get("isOtmEnabled", ""),
            "SN Number": sn_list
        })

    # 4. FULFILLMENT
    fulfillment = result.get("fulfillment")
    fulfillment_id = None
    if isinstance(fulfillment, dict):
        fulfillment_id = fulfillment.get("fulfillmentId")
    elif isinstance(fulfillment, list) and fulfillment:
        fulfillment_id = fulfillment[0].get("fulfillmentId")

    if fulfillment_id:
        combined_data["data"]["getFulfillmentsById"] = (
            post_api(SOPATH, fetch_fulfillment_query(), {"fulfillment_id": fulfillment_id})
            .get("data", {})
            .get("getFulfillmentsById", [])
        )
        combined_data["data"]["getFulfillmentsBysofulfillmentid"] = (
            post_api(SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id))
            .get("data", {})
            .get("getFulfillmentsBysofulfillmentid", [])
        )
        combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"] = (
            post_api(FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id))
            .get("data", {})
            .get("getAllFulfillmentHeadersSoidFulfillmentid", [])
        )
        combined_data["data"]["getFbomBySoFulfillmentid"] = (
            post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id))
            .get("data", {})
            .get("getFbomBySoFulfillmentid", [])
        )

    # 5. FOID
    fulfillment_orders = result.get("fulfillmentOrders", [])
    if fulfillment_orders:
        foid = fulfillment_orders[0].get("foId")
        if foid:
            fo_output = post_api(FOID, fetch_foid_query(foid))
            fo_result = fo_output.get("data", {}).get("getAllFulfillmentHeadersByFoId", [])
            combined_data["data"]["getAllFulfillmentHeadersByFoId"] = fo_result

    return combined_data
