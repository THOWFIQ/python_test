for idx in range(N):
            for wo in safe_get(salesorders[idx], ['workOrders'], []):
                wo_id = safe_get(wo, ['woId'])
                if wo_id not in tempwoid:
                    tempwoid.append(wo_id)
            
            shipping_addr = pick_address_by_type(salesheaders_by_ids[idx], "SHIPPING")
            billing_addr = pick_address_by_type(salesheaders_by_ids[idx], "BILLING")

            ASN, Destination, Origin, Way_Bill_Number, ship_mode = "", "", "", "", ""
            ActualShipCode, OrderVolWt, PPID, SvcTag, TargetDeliveryDate, TotalBoxCount, TotalGrossWeight, TotalVolumetricWeight,as_shipped_ppid,make_man_dtls,make_man_ppids = "", "", "", "", "", "", "", "", "", "", ""
            DellBlanketPoNum, IsLastLeg, ShipFromMcid, WoOtmEnable, WoShipMode,wo_lines,has_software,MakeWoAckValue,McidValue,WO_ID  = "", "", "", "", "","","","","",""
            
            if safe_get(salesorders[idx], ['asnNumbers', 0, 'sourceManifestId']) != "":
                for asheaderData in ASNheaderByID:
                    if (asheaderData.get('shipFromVendorId') == safe_get(salesorders[idx], ['asnNumbers', 0, 'shipFromVendorId'])
                        and (asheaderData.get('sourceManifestId') == safe_get(salesorders[idx], ['asnNumbers', 0, 'sourceManifestId']))):                        
                        ASN             = safe_get(asheaderData, ['sourceManifestId'])
                        Destination     = safe_get(asheaderData, ['shipToVendorSiteId'])
                        Origin          = safe_get(asheaderData, ['shipFromVendorSiteId'])
                        Way_Bill_Number = safe_get(asheaderData, ['airwayBillNum'])
                        ship_mode       = safe_get(asheaderData, ['shipMode'])

                for asdetailData in ASNDetailById:
                    if asdetailData.get('sourceManifestId') == safe_get(salesorders[idx], ['asnNumbers', 0, 'sourceManifestId']):
                        ActualShipCode        = safe_get(asdetailData,['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'shipviaCode'])
                        OrderVolWt            = safe_get(asdetailData,['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'boxVolWt'])
                        box_details           = safe_get(asdetailData, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails'])
                        PPID                  = safe_get(box_details[0], ['basePpid'])
                        SvcTag                = list(filter(
                                                        lambda tag: tag is not None,
                                                        map(
                                                            lambda detail: safe_get(detail, ['serviceTag']),
                                                            sum([
                                                                safe_get(box, ['woShipmentBoxDetails']) or []
                                                                for pallet in safe_get(asdetailData, ['manifestPallet']) or []
                                                                for shipment in safe_get(pallet, ['woShipment']) or []
                                                                for box in safe_get(shipment, ['woShipmentBox']) or []
                                                            ], [])
                                                        )
                                                    ))
                        TargetDeliveryDate    = safe_get(asdetailData,['manifestPallet', 0, 'woShipment', 0, 'estDeliveryDate'])
                        
                        shipment_boxes        = sum([safe_get(shipment, ['woShipmentBox']) or []
                                                    for pallet in safe_get(asdetailData, ['manifestPallet']) or []
                                                    for shipment in safe_get(pallet, ['woShipment']) or []
                                                ], [])

                        TotalBoxCount         = len(list(filter(lambda box: safe_get(box, ['boxRef']) is not None, shipment_boxes)))
                        TotalGrossWeight      = sum(filter(lambda wt: wt is not None, map(lambda box: safe_get(box, ['boxGrossWt'], default=0), shipment_boxes)))
                        TotalVolumetricWeight = sum(filter(lambda wt: wt is not None, map(lambda box: safe_get(box, ['boxVolWt'], default=0), shipment_boxes)))
                        as_shipped_ppid       = safe_get(box_details[0], ['asShippedPpid'])
                        make_man_dtls         = safe_get(box_details[0], ['woMakeManDtl'])
                        make_man_ppids        = list(filter(lambda ppid: ppid is not None,map(lambda detail: safe_get(detail, ['asShippedPpid']), make_man_dtls)))
            
            print(f"temp id : {tempwoid}")
            if len(safe_get(salesorders[idx], ['workOrders'], [])) > 0:
                index = index+1
                # print(f"salesn wo lenght :{len(safe_get(salesorders[idx], ['workOrders'], []))}")
                # print(f"wo lenght :{len(WorkOrderByID)}")
                # print(f"index value : {[idx]}")
                # index = len(WorkOrderByID)
                
                for WorkOrderData in WorkOrderByID:
                    # print(f"Index: {index}, WorkOrder ID: {WorkOrderData.get('woId')}")
                    # print(f"after the indec : {index}")
                    # print(f"oooooo: {tempwoid}")
                    if WorkOrderData.get('woId') not in tempwoid:
                        tempwoid.remove(safe_get(WorkOrderData, ['woId']))
                        print(f"Matched reverse index: {index}")
                        WO_ID = safe_get(WorkOrderData, ['woId'])
                        DellBlanketPoNum = safe_get(WorkOrderData, ['dellBlanketPoNum'])
                        IsLastLeg = safe_get(WorkOrderData, ['shipToFacility'])
                        ShipFromMcid = safe_get(WorkOrderData, ['vendorSiteId'])
                        WoOtmEnable = safe_get(WorkOrderData, ['isOtmEnabled'])
                        WoShipMode = safe_get(WorkOrderData, ['shipMode'])
                        wo_lines = safe_get(WorkOrderData, ['woLines'])
                        has_software = any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in wo_lines)
                        MakeWoAckValue = (
                                            "True" if WorkOrderData.get('woType') == 'MAKE' and
                                            any(str(status.get('channelStatusCode')) == '3000'
                                                for status in WorkOrderData.get('woStatusList', []))
                                            else "False"
                                        )
                        McidValue = (
                                        WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                                        WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                                    ) 
           
                        # tempwoid.remove(tempwoid[0])                        
                        
            ship_first = shipping_addr.get("firstName", "") if shipping_addr else ""
            ship_last = shipping_addr.get("lastName", "") if shipping_addr else ""
            shipping_contact_name = (f"{ship_first} {ship_last}").strip()

            salesData = safe_get(salesorders[idx], ['salesOrder', 'salesOrderId'])
            
            row = {
                "Fulfillment ID": safe_get(salesorders[idx], ['fulfillment', 0, 'fulfillmentId']),
                "BUID": safe_get(salesorders[idx], ['salesOrder', 'buid']),
                "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                "InstallInstruction2": get_install_instruction2_id(fulfillments_by_id[idx]),
                "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                "ShippingContactName": shipping_contact_name,
                "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                "ShipToPhone": (listify(shipping_addr.get("phone", []))[0].get("phoneNumber", "")
                                if shipping_addr and listify(shipping_addr.get("phone", [])) else ""),
                "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                "PP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "PP" else "",
                "IP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "IP" else "",
                "MN Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "MN" else "",
                "SC Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "SC" else "",
                "CFI Flag": safe_get(VendormasterByVendor[idx], ['isCfi']) if VendormasterByVendor and 0 <= idx < len(VendormasterByVendor) else "N",
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
                "Source System Status":  safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
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
                "Sales Order ID": safe_get(salesorders[idx], ['salesOrder', 'salesOrderId']),                        
                "Region Code": safe_get(salesorders[idx], ['salesOrder', 'region']),
                "FO ID": safe_get(salesorders[idx], ['fulfillmentOrders', 0, 'foId']),
                "WO ID": WO_ID,
                "System Qty": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'systemQty']),
                "Ship By Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipByDate']),
                "LOB": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
                # "Ship From Facility": safe_get(salesorders[idx], ['asnNumbers', 0, 'shipFrom']),
                # "Ship To Facility": safe_get(salesorders[idx], ['asnNumbers', 0, 'shipTo']),
                "Facility": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'facility']),
                "SN Number": safe_get(salesorders[idx], ['asnNumbers', 0, 'snNumber']),
                "Tax Regstrn Num": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'taxRegstrnNum']),
                "State Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'stateCode']),
                "City Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'cityCode']),
                "Customer Num": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'customerNum']),
                "Customer Name Ext": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'customerNameExt']),
                "Country": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'country']),
                "Create Date": dateFormation(dateFormation(safe_get(salesheaders_by_ids[idx], ['createDate']))),
                "Ship Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipCode']),
                "Must Arrive By Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mustArriveByDate'])),
                "Update Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'updateDate'])),
                "Merge Type": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mergeType']),
                "Manifest Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'manifestDate'])),
                "Revised Delivery Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'revisedDeliveryDate'])),
                "Delivery City": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'deliveryCity']),
                "Source System ID": safe_get(fulfillments_by_id[idx], ['sourceSystemId']),
                "OIC ID": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'oicId']),
                "Order Date": dateFormation(safe_get(salesheaders_by_ids[idx], ['orderDate'])),
                "Actual Ship Mode":ship_mode,
                "First Leg Ship Mode":ship_mode,
                "ASN":ASN,
                "Destination":Destination,
                "Manifest ID":ASN,
                "Origin":Origin,
                "Way Bill Number":Way_Bill_Number,                   
                "Build Facility":"",
                "Dell Blanket Po Num": DellBlanketPoNum,
                "Has Software":has_software,
                "Is Last Leg": IsLastLeg,
                "Make WoAck": MakeWoAckValue,                
                "Mcid": McidValue,
                "Ship From Mcid": ShipFromMcid,
                "Ship To Mcid": IsLastLeg,
                "Wo Otm Enable": WoOtmEnable,
                "Wo Ship Mode":WoShipMode,
                "Actual Ship Code": ActualShipCode,
                "Order Vol Wt": OrderVolWt,
                "PP ID": PPID,
                "Svc Tag": SvcTag,
                "Target Delivery Date": TargetDeliveryDate,
                "Total Box Count": TotalBoxCount,
                "Total Gross Weight": TotalGrossWeight,
                "Total Volumetric Weight": TotalVolumetricWeight,
                "Order Type": safe_get(salesheaders_by_ids[idx], ['orderType']),
            }

            flat_list.append(row)
