def chunk_list(data_list, chunk_size):
    """Split list into chunks of given size."""
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]


def mainfunction(filters, format_type, region):
    payload = {}
    path = getPath(region)  
    records = []

    # --- Handle "from" & "to" date case ---
    if "from" in filters and "to" in filters:
        url = path['SOPATH']
        payload = {
            "query": fetch_getOrderDate_query(
                filters["from"],
                filters["to"],
                filters.get("fulfillmentSts", ""),
                filters.get("sourceSystemSts", "")
            )
        }
        response = requests.post(url, json=payload, verify=False)
        data = response.json()
        if "errors" in data:
            return jsonify({"error": data["errors"]}), 500
        result = data.get("data", {}).get("getOrdersByDate", {})
        for entry in result.get("result", []):
            record = OrderDateRecord(
                salesOrderId=SalesOrderIdData(entry.get("salesOrderId", {})),
                fulfillmentId=FulfillmentIdData(entry.get("fulfillmentId", {}))
            )
            records.append(record)

    # --- Handle "Sales_Order_id" with batching ---
    if "Sales_Order_id" in filters:
        url = path['FID']
        sales_ids = list(map(str.strip, filters["Sales_Order_id"].split(",")))
        for batch in chunk_list(sales_ids, 50):
            payload = {
                "query": fetch_salesorderf_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 500
            result = data.get("data", {}).get("getBySalesorderids", {})
            for entry in result.get("result", []):
                record = SalesRecord(
                    asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])],
                    fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
                )
                records.append(record)

    # --- Handle "Fullfillment Id" with batching ---
    if "Fullfillment Id" in filters:
        url = path['FID']
        fulfillment_ids = list(map(str.strip, filters["Fullfillment Id"].split(",")))
        for batch in chunk_list(fulfillment_ids, 50):
            payload = {
                "query": fetch_getByFulfillmentids_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 500
            result = data.get("data", {}).get("getByFulfillmentids", {})
            for entry in result.get("result", []):
                record = FulfillmentRecord(
                    asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])],
                    fulfillment=Fulfillment(**entry.get("fulfillment", {})),
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
                )
                records.append(record)

    # --- Handle "foid" with batching ---
    if "foid" in filters:
        url = path['FID']
        fo_ids = list(map(str.strip, filters["foid"].split(",")))
        for batch in chunk_list(fo_ids, 50):
            payload = {
                "query": fetch_getByFoid_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 500
            result = data.get("data", {}).get("getByFoid", {})
            for entry in result.get("result", []):
                record = FulfillmentOrderRecord(
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
                )
                records.append(record)

    # --- Handle "wo_id" with batching ---
    if "wo_id" in filters:
        url = path['FID']
        wo_ids = list(map(str.strip, filters["wo_id"].split(",")))
        for batch in chunk_list(wo_ids, 50):
            payload = {
                "query": fetch_getByWoId_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 500
            result = data.get("data", {}).get("getByWoId", {})
            for entry in result.get("result", []):
                record = WorkOrderRecord(
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])]
                )
                records.append(record)

    # --- Build GraphQL request list for async processing ---
    graphql_request = []
    countReqNo = 0
    for obj in records:
        countReqNo += 1

        # Sales Order Id requests
        if obj.salesOrderId and obj.salesOrderId.salesOrderId:
            graphql_request.append({
                "url": path['FID'],
                "query": fetch_salesorder_query(json.dumps(obj.salesOrderId.salesOrderId))
            })
            print(f"[{countReqNo}] Sales Order: {obj.salesOrderId.salesOrderId}")

        # Fulfillment Id requests
        fulfillment_id = None
        if hasattr(obj, "fulfillmentId"):
            fulfillment_id = getattr(obj.fulfillmentId, "fulfillmentId", obj.fulfillmentId)
        elif hasattr(obj, "fulfillment") and obj.fulfillment:
            if isinstance(obj.fulfillment, Fulfillment):
                fulfillment_id = obj.fulfillment.fulfillmentId
            elif isinstance(obj.fulfillment, list) and isinstance(obj.fulfillment[0], Fulfillment):
                fulfillment_id = obj.fulfillment[0].fulfillmentId

        if fulfillment_id:
            graphql_request.append({
                "url": path['SOPATH'],
                "query": fetch_fulfillmentf_query(json.dumps(fulfillment_id),
                                                  json.dumps(obj.salesOrderId.salesOrderId))
            })
            graphql_request.append({
                "url": path['FID'],
                "query": fetch_getByFulfillmentids_query(json.dumps(fulfillment_id))
            })
            print(f"[{countReqNo}] Fulfillment ID: {fulfillment_id}")

    # --- Run async requests ---
    results = asyncio.run(run_all(graphql_request))
    return results
