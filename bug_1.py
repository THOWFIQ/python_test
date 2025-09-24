def newOutputFormat(result_map, format_type=None, region=None):
    try:
        final_output = []

        for item in result_map:
            data = item.get("data", {})

            # Handle SalesOrder + Fulfillment
            salesorders = data.get("getBySalesorderids", {}).get("result", [])
            fulfillments = data.get("getByFulfillmentIds", {}).get("result", [])

            # Handle WorkOrders (safe lookup)
            workorders = []
            if "getWorkOrderByWoIds" in data:
                if isinstance(data["getWorkOrderByWoIds"], dict):
                    workorders = data["getWorkOrderByWoIds"].get("result", [])
                elif isinstance(data["getWorkOrderByWoIds"], list):
                    workorders = data["getWorkOrderByWoIds"]

            # Now merge into row
            for so in salesorders:
                row = {
                    "SalesOrderId": so.get("salesOrder", {}).get("salesOrderId"),
                    "Region": so.get("salesOrder", {}).get("region"),
                }

                # Add fulfillment fields
                if fulfillments:
                    f = fulfillments[0]  # example: take first
                    row.update({
                        "FulfillmentId": f.get("fulfillmentId"),
                        "Status": f.get("status"),
                    })

                # Add workorder fields
                if workorders:
                    w = workorders[0]  # example: take first
                    row.update({
                        "WorkOrderId": w.get("woId"),
                        "CustomerName": w.get("customer", {}).get("companyName"),
                        "ShipToFacility": w.get("shipToFacility", {}).get("facilityName"),
                    })

                final_output.append(row)

        return final_output

    except Exception as e:
        print("Error in newOutputFormat:", str(e))
        return []
