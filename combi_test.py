def newmainfunction(filters, format_type, region):
    """
    Main function to fetch Sales Orders, Fulfillments, and Work Orders based on filters.
    Returns combined results from all GraphQL queries asynchronously.
    """
    region = region.upper()
    path = getPath(region)

    graphql_request = []
    finalResult = []

    # ----------- Fullfillment IDs -----------
    if "Fullfillment Id" in filters:
        fulfillment_key = "Fullfillment Id"
        uniqueFullfillment_ids = ",".join(sorted(set(filters[fulfillment_key].split(','))))
        filters[fulfillment_key] = uniqueFullfillment_ids

        if filters.get(fulfillment_key):
            fulfillment_ids = list(map(str.strip, filters[fulfillment_key].split(",")))
            for ffid in chunk_list(fulfillment_ids, 10):
                graphql_request.append({
                    "url": path['SALESFULLFILLMENT'],
                    "query": fetch_Fullfillment_query(ffid)
                })

    # ----------- Sales Orders -----------
    if "Sales_Order_id" in filters:
        salesOrder_key = "Sales_Order_id"
        uniqueSalesOrder_ids = ",".join(sorted(set(filters[salesOrder_key].split(','))))
        filters[salesOrder_key] = uniqueSalesOrder_ids

        if filters.get(salesOrder_key):
            salesorder_ids = list(map(str.strip, filters[salesOrder_key].split(",")))

            # Fetch Sales Orders synchronously first to get related Work Orders
            for soid_chunk in chunk_list(salesorder_ids, 10):
                payload = {"query": fetch_keysphereSalesorder_query(soid_chunk)}
                response = requests.post(path['FID'], json=payload, verify=False)
                data = response.json()

                if "errors" in data:
                    continue

                result = data.get("data", {}).get("getBySalesorderids", {})
                new_items = list(map(
                    lambda item: {
                        "salesOrderId": item.get("salesOrder", {}).get("salesOrderId"),
                        "region": item.get("salesOrder", {}).get("region"),
                        "workOrderIds": list(map(
                            lambda wo: wo.get("woId"),
                            filter(lambda wo: wo.get("woId"), item.get("workOrders", []))
                        ))
                    },
                    result.get("result", [])
                ))

                finalResult.extend(new_items)

                # Collect all Work Order IDs
                work_order_ids = ",".join(sorted(set(map(
                    lambda wo: wo.get("woId"),
                    [wo for item in result.get("result", []) for wo in item.get("workOrders", []) if wo.get("woId")]
                ))))
                wo_ids = list(map(str.strip, work_order_ids.split(",")))

                # Add async GraphQL requests for Fulfillment & Work Orders
                graphql_request.append({
                    "url": path['SALESFULLFILLMENT'],
                    "query": fetch_salesOrder_query(soid_chunk)
                })

                if wo_ids:
                    graphql_request.append({
                        "url": path['WORKORDER'],
                        "query": fetch_workOrder_query(wo_ids)
                    })

    # ----------- Work Orders only (if provided directly) -----------
    if "wo_id" in filters:
        workOrder_key = "wo_id"
        uniqueWorkOrder_ids = ",".join(sorted(set(filters[workOrder_key].split(','))))
        filters[workOrder_key] = uniqueWorkOrder_ids

        if filters.get(workOrder_key):
            workorder_ids = list(map(str.strip, filters[workOrder_key].split(",")))
            for woid_chunk in chunk_list(workorder_ids, 10):
                graphql_request.append({
                    "url": path['WORKORDER'],
                    "query": fetch_workOrder_query(woid_chunk)
                })

    # ----------- Run all async GraphQL requests -----------
    results = asyncio.run(run_all(graphql_request))

    # Return both detailed GraphQL results and summary of Sales Orders
    return {
        "sales_orders_summary": finalResult,
        "graphql_details": results
    }
