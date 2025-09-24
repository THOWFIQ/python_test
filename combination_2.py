flat_list = []
ValidCount = []

for item in result_map:
    data = item.get("data")
    if not data:
        continue

    # --- Sales Orders ---
    sales_orders = data.get("getSalesOrderBySoids", {}).get("salesOrders", [])
    for so in sales_orders:
        fulfillments = listify(so.get("fulfillments", []))
        workorders_from_so = listify(so.get("workOrders", []))  # in case wo inside so
        # Process sales order row here (your row building code)
        row = {
            "Sales Order ID": so.get("salesOrderId"),
            "BUID": so.get("buid"),
            "Amount": so.get("totalPrice"),
            "Currency Code": so.get("currency"),
            # ... add all your fields ...
        }
        flat_list.append(row)

    # --- Work Orders ---
    work_orders = data.get("getWorkOrderByWoIds", [])
    for wo in work_orders:
        WO_ID = wo.get("woId")
        DellBlanketPoNum = wo.get("dellBlanketPoNum")
        ship_to_facility = wo.get("shipToFacility")


           results = asyncio.run(run_all(graphql_request))

    # Return both detailed GraphQL results and summary of Sales Orders
    return {
        "sales_orders_summary": finalResult,
        "graphql_details": results
    }
        IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
        ShipFromMcid = wo.get("vendorSiteId")
        WoOtmEnable = wo.get("isOtmEnabled")
        WoShipMode = wo.get("shipMode")
        wo_lines = wo.get("woLines", [])
        ismultipack = wo_lines[0].get("ismultipack") if wo_lines else ""
        has_software = any(line.get("woLineType") == "SOFTWARE" for line in wo_lines)
        MakeWoAckValue = next(
            (status.get("statusDate") for status in wo.get("woStatusList", [])
             if str(status.get("channelStatusCode")) == "3000" and wo.get("woType") == "MAKE"), ""
        )
        McidValue = (
            wo.get('woShipInstr', [{}])[0].get('mergeFacility') or
            wo.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
        )

        wo_row = {
            "WO_ID": WO_ID,
            "DellBlanketPoNum": DellBlanketPoNum,
            "Ship To Facility": ship_to_facility,
            "Is Last Leg": IsLastLeg,
            "Ship From MCID": ShipFromMcid,
            "Otm Enabled": WoOtmEnable,
            "Ship Mode": WoShipMode,
            "Is Multipack": ismultipack,
            "Has Software": has_software,
            "Make WO Ack": MakeWoAckValue,
            "MCID Value": McidValue
        }
        flat_list.append(wo_row)

for idx, item in enumerate(result_map):
    print("Item index:", idx, "type:", type(item))
    if not isinstance(item, dict):
        print("Skipping non-dict item:", item)
        continue

    data = item.get("data")
    if not data:
        print("No 'data' key in item:", item)
        continue

    print("break point - data exists")

Item index: 0 type: <class 'str'>
Skipping non-dict item: sales_orders_summary
Item index: 1 type: <class 'str'>
Skipping non-dict item: graphql_details

   results = asyncio.run(run_all(graphql_request))

    # Return both detailed GraphQL results and summary of Sales Orders
    return {
        "sales_orders_summary": finalResult,
        "graphql_details": results
    }


def outputformat(result_map, format_type=None, filtersValue=None, region=None):
    try:
        flat_list = []
        ValidCount = []

        # Only use GraphQL details
        graphql_details = result_map.get("graphql_details", [])

        for item in graphql_details:
            data = item.get("data")  # actual GraphQL response
            if not data:
                continue

            # Handle Sales Orders
            sales_orders = data.get("getSalesOrderBySoids", {}).get("salesOrders", [])
            for so in sales_orders:
                fulfillments = so.get("fulfillments", [])
                workorders = []  # We'll attach Work Orders later

                # Collect Work Orders from another GraphQL response
                wo_data_list = data.get("getWorkOrderByWoIds", [])
                workorders.extend(wo_data_list)

                # ------------------- Sales Order Row -------------------
                for fulfillment in fulfillments:
                    row = {
                        "Sales Order ID": so.get("salesOrderId"),
                        "Fulfillment ID": fulfillment.get("fulfillmentId"),
                        "BUID": so.get("buid"),
                        "CustomerName": "",
                        "LOB": ", ".join([line.get("lob") or "" for line in fulfillment.get("salesOrderLines", [])]),
                        "Facility": ", ".join([line.get("facility") or "" for line in fulfillment.get("salesOrderLines", [])]),
                        "Delivery City": fulfillment.get("deliveryCity"),
                        "Amount": so.get("totalPrice"),
                        "Currency Code": so.get("currency")
                        # add other fields as needed
                    }
                    flat_list.append(row)

                # ------------------- Work Orders Rows -------------------
                for WorkOrderData in workorders:
                    WO_ID = WorkOrderData.get('woId')
                    DellBlanketPoNum = WorkOrderData.get('dellBlanketPoNum')
                    ship_to_facility = WorkOrderData.get('shipToFacility')
                    IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
                    ShipFromMcid = WorkOrderData.get('vendorSiteId')
                    WoOtmEnable = WorkOrderData.get('isOtmEnabled')
                    WoShipMode = WorkOrderData.get('shipMode')
                    wo_lines = WorkOrderData.get('woLines', [])
                    ismultipack = wo_lines[0].get("ismultipack") if wo_lines else ""
                    has_software = any(line.get('woLineType') == 'SOFTWARE' for line in wo_lines)
                    MakeWoAckValue = next(
                        (status.get("statusDate") for status in WorkOrderData.get("woStatusList", [])
                         if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"),
                        ""
                    )
                    McidValue = (
                        WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                        WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                    )

                    wo_row = {
                        "Sales Order ID": so.get("salesOrderId"),
                        "WO_ID": WO_ID,
                        "DellBlanketPoNum": DellBlanketPoNum,
                        "Ship To Facility": ship_to_facility,
                        "Is Last Leg": IsLastLeg,
                        "Ship From MCID": ShipFromMcid,
                        "Otm Enabled": WoOtmEnable,
                        "Ship Mode": WoShipMode,
                        "Is Multipack": ismultipack,
                        "Has Software": has_software,
                        "Make WO Ack": MakeWoAckValue,
                        "MCID Value": McidValue
                    }
                    flat_list.append(wo_row)

        if not flat_list:
            return {"error": "No Data Found"}

        if format_type == "export":
            return flat_list
        elif format_type == "grid":
            return flat_list

        return flat_list

    except Exception as e:
        return {"error": str(e)}

