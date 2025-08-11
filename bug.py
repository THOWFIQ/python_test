def OutputFormat(result_map, format_type=None, region=None):
    try:
        # Extract relevant blocks
        salesorders = list(map(
            lambda item: item["data"]["getBySalesorderids"]["result"][0],
            filter(lambda item: "getBySalesorderids" in item.get("data", {}), result_map)
        ))

        fulfillments_by_id = list(map(
            lambda item: item["data"]["getFulfillmentsById"][0],
            filter(lambda item: "getFulfillmentsById" in item.get("data", {}), result_map)
        ))

        salesheaders_by_ids = list(map(
            lambda item: item["data"]["getSoheaderBySoids"][0],
            filter(lambda item: "getSoheaderBySoids" in item.get("data", {}), result_map)
        ))

        # Combine all entries
        flat_list = list(map(lambda idx: {
            "BUID": safe_get(salesorders[idx], ['salesOrder', 'buid']),
            "PP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "PP" else "",
            "IP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "IP" else "",
            "MN Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "MN" else "",
            "SC Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "SC" else "",
            "CFI Flag": "",
            "Agreement ID": safe_get(salesheaders_by_ids[idx], ['agreementId']),
            "Amount": safe_get(salesheaders_by_ids[idx], ['totalPrice']),
            "Currency Code": safe_get(salesheaders_by_ids[idx], ['currency']),
            "Customer Po Number": safe_get(salesheaders_by_ids[idx], ['poNumber']),
            "Dp ID": safe_get(salesheaders_by_ids[idx], ['dpid']),
            "Location Number": safe_get(salesheaders_by_ids[idx], ['locationNum']),
            "Order Age": safe_get(salesheaders_by_ids[idx], ['orderDate']),
            "Order Amount usd": safe_get(salesheaders_by_ids[idx], ['rateUsdTransactional']),
            "Order Update Date": safe_get(salesheaders_by_ids[idx], ['updateDate']),
            "Rate Usd Transactional": safe_get(salesheaders_by_ids[idx], ['rateUsdTransactional']),
            "Sales Rep Name": safe_get(salesheaders_by_ids[idx], ['salesrep', 0, 'salesRepName']),
            "Shipping Country": safe_get(salesheaders_by_ids[idx], ['address', 0, 'country']),
            "Source System Status": safe_get(fulfillments_by_id[idx], ['sourceSystemId']),
            "Tie Number": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'soLineNum']),
            "Si Number": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'siNumber']),
            "Req Ship Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipCode']),
            "Reassigned Ip Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
            "RDD": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'revisedDeliveryDate']),
            "Product Lob": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
            "Payment Term Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'paymentTerm']),
            "Ofs Status Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
            "Ofs Status": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
            "Fulfillment Status": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
            "DomsStatus": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
            "Company Name": safe_get(salesheaders_by_ids[idx], ['address', 0, 'companyName']),
            "Contact Type": safe_get(salesheaders_by_ids[idx], ['address', 0, 'contact', 0, 'contactType']),
            "Shipping City Code": safe_get(salesheaders_by_ids[idx], ['address', 0, 'cityCode']),
            "City": safe_get(salesheaders_by_ids[idx], ['address', 0, 'city']),
            "First Name": safe_get(salesheaders_by_ids[idx], ['address', 0, 'firstName']),
            "Last Name": safe_get(salesheaders_by_ids[idx], ['address', 0, 'lastName']),
            "State Code": safe_get(salesheaders_by_ids[idx], ['address', 0, 'stateCode']),
            "Address Line1": safe_get(salesheaders_by_ids[idx], ['address', 0, 'addressLine1']),
            "Address Line2": safe_get(salesheaders_by_ids[idx], ['address', 0, 'addressLine2']),
            "Phone Number": safe_get(salesheaders_by_ids[idx], ['address', 0, 'phone', 0, 'phoneNumber']),
            "Postal Code": safe_get(salesheaders_by_ids[idx], ['address', 0, 'postalCode']),
            "Sales Order ID": safe_get(salesorders[idx], ['salesOrder', 'salesOrderId']),
            "Fulfillment ID": safe_get(salesorders[idx], ['fulfillment', 0, 'fulfillmentId']),
            "Region Code": safe_get(salesorders[idx], ['salesOrder', 'region']),
            "FO ID": safe_get(salesorders[idx], ['fulfillmentOrders', 0, 'foId']),
            "WO ID": safe_get(salesorders[idx], ['workOrders', 0, 'woId']),
            "System Qty": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'systemQty']),
            "Ship By Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipByDate']),
            "LOB": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
            "Facility": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'facility']),
            "Tax Regstrn Num": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'taxRegstrnNum']),
            "Create Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'createDate'])),
            "Ship Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipCode']),
            "Must Arrive By Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mustArriveByDate'])),
            "Update Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'updateDate'])),
            "Merge Type": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mergeType']),
            "Manifest Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'manifestDate'])),
            "Revised Delivery Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'revisedDeliveryDate'])),
            "Delivery City": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'deliveryCity']),
            "Source System ID": safe_get(fulfillments_by_id[idx], ['sourceSystemId']),
            "OIC ID": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'oicId']),
            "Order Date": dateFormation(safe_get(salesorders[idx], ['salesOrder', 'createDate']))
        }, range(min(len(salesorders), len(fulfillments_by_id), len(salesheaders_by_ids)))))

        if not flat_list:
            return {"Error Message": "No Data Found"}

        if format_type == "export":
            return flat_list

        elif format_type == "grid":
            # Extract column order dynamically from the first item
            desired_order = list(flat_list[0].keys())
            rows = []

            for item in flat_list:
                reordered_values = [item.get(key) for key in desired_order]
                row = {
                    "columns": [{"value": val if val is not None else ""} for val in reordered_values]
                }
                rows.append(row)

            table_grid_output = tablestructural(rows, region) if rows else []
            return table_grid_output

        else:
            return {"error": "Format type must be either 'grid' or 'export'"}

    except Exception as e:
        print(f"[ERROR] OutputFormat failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}
