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
