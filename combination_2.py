WO_ID = safe_get(WorkOrderData, ['woId'])
                                DellBlanketPoNum = safe_get(WorkOrderData, ['dellBlanketPoNum'])
                                ship_to_facility = safe_get(WorkOrderData, ['shipToFacility'])
                                IsLastLeg = 'Y' if ship_to_facility and 'CUST' in ship_to_facility.upper() else 'N'
                                ShipFromMcid = safe_get(WorkOrderData, ['vendorSiteId'])
                                WoOtmEnable = safe_get(WorkOrderData, ['isOtmEnabled'])
                                WoShipMode = safe_get(WorkOrderData, ['shipMode'])
                                ismultipack = safe_get(WorkOrderData, ['woLines',0,"ismultipack"])
                                wo_lines = safe_get(WorkOrderData, ['woLines'])
                                has_software = any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in wo_lines)
                                MakeWoAckValue = next((dateFormation(status.get("statusDate")) for status in WorkOrderData.get("woStatusList", [])
                                                        if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"),
                                                        "")
                                McidValue = (
                                                WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                                                WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                                            )
