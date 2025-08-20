def OutputFormat(result_map, format_type=None, region=None):
    try:
        salesorders = [
            item["data"]["getBySalesorderids"]["result"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getBySalesorderids" in item["data"]
            and isinstance(item["data"]["getBySalesorderids"].get("result"), list)
            and item["data"]["getBySalesorderids"]["result"]
        ]

        fulfillments_by_id = [
            item["data"]["getFulfillmentsById"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getFulfillmentsById" in item["data"]
            and isinstance(item["data"]["getFulfillmentsById"], list)
            and item["data"]["getFulfillmentsById"]
        ]

        salesheaders_by_ids = [
            item["data"]["getSoheaderBySoids"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getSoheaderBySoids" in item["data"]
            and isinstance(item["data"]["getSoheaderBySoids"], list)
            and item["data"]["getSoheaderBySoids"]
        ]

        VendormasterByVendor = [
            item["data"]["getVendormasterByVendorsiteid"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getVendormasterByVendorsiteid" in item["data"]
            and isinstance(item["data"]["getVendormasterByVendorsiteid"], list)
            and item["data"]["getVendormasterByVendorsiteid"]
        ]

        ASNheaderByID = [
            item["data"]["getAsnHeaderById"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getAsnHeaderById" in item["data"]
            and isinstance(item["data"]["getAsnHeaderById"], list)
            and item["data"]["getAsnHeaderById"]
        ]

        WorkOrderByID = [
            item["data"]["getWorkOrderById"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getWorkOrderById" in item["data"]
            and isinstance(item["data"]["getWorkOrderById"], list)
            and item["data"]["getWorkOrderById"]
        ]

        ASNDetailById = [
            item["data"]["getAsnDetailById"]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getAsnDetailById" in item["data"]
            and isinstance(item["data"]["getAsnDetailById"], dict)
        ]
        
        def listify(x):
            if x is None:
                return []
            if isinstance(x, list):
                return x
            return [x]

        def pick_address_by_type(salesheader_entry: Dict, contact_type: str) -> Dict:
            
            addresses = salesheader_entry.get("address", [])
            addresses = listify(addresses)
            for addr in addresses:
                contacts = listify(addr.get("contact", []))
                for c in contacts:
                    if isinstance(c, dict) and c.get("contactType") == contact_type:
                        return addr
            return {}

        def get_install_instruction2_id(fulfillment_entry: Dict) -> str:
           
            fulfills = listify(fulfillment_entry.get("fulfillments", []))
            if not fulfills:
                return ""
            lines = listify(fulfills[0].get("salesOrderLines", []))
            for line in lines:
                instrs = listify(line.get("specialinstructions", []))
                for instr in instrs:
                    if instr.get("specialInstructionType") == "INSTALL_INSTR2":
                        return str(instr.get("specialInstructionId", ""))
            return ""

        # ---------- flat list ----------
        print(f" sales order count :{len(salesorders)}")
        print(f" fullfullment count : {len(fulfillments_by_id)}")
        print(f" sales header count : {len(salesheaders_by_ids)}")
        print(f" vendor count : {len(VendormasterByVendor)}")
        print(f" asn header count : {len(ASNheaderByID)}")
        print(f" work count : {len(WorkOrderByID)}")
        print(f" asn details count : {len(ASNDetailById)}")
        exit()
        N = min(len(salesorders), len(fulfillments_by_id), len(salesheaders_by_ids))
        # ,len(VendormasterByVendor),len(ASNheaderByID),len(WorkOrderByID),len(ASNDetailById))
        flat_list = []       
        for idx in range(N):
            shipping_addr = pick_address_by_type(salesheaders_by_ids[idx], "SHIPPING")
            billing_addr = pick_address_by_type(salesheaders_by_ids[idx], "BILLING")

            ship_first = shipping_addr.get("firstName", "") if shipping_addr else ""
            ship_last = shipping_addr.get("lastName", "") if shipping_addr else ""
            shipping_contact_name = (f"{ship_first} {ship_last}").strip()

            wo_lines = safe_get(WorkOrderByID[idx] if idx < len(WorkOrderByID) else {}, ['woLines'], default=[])
            
            has_software = any(
                map(lambda line: safe_get(line, ['woLineType']) == 'SOFTWARE', wo_lines)
            )

            MakeWoAckValue = (
                "True" if WorkOrderByID and
                idx < len(WorkOrderByID) and
                WorkOrderByID[idx].get('woType') == 'MAKE' and
                any(str(status.get('channelStatusCode')) == '3000' for status in WorkOrderByID[idx].get('woStatusList', []))
                else "False"
            )

            McidValue = (
                WorkOrderByID[idx].get('woShipInstr', [{}])[0].get('mergeFacility')
                if idx < len(WorkOrderByID) and WorkOrderByID[idx].get('woShipInstr', [{}])[0].get('mergeFacility')
                else WorkOrderByID[idx].get('woShipInstr', [{}])[0].get('carrierHubCode', "") if idx < len(WorkOrderByID) else ""
            )

            box_details = safe_get(ASNDetailById[idx] if idx < len(ASNDetailById) else {}, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails'], default=[])

            base_ppid = safe_get(box_details[0], ['basePpid']) if box_details else None
            as_shipped_ppid = safe_get(box_details[0], ['asShippedPpid']) if box_details else None

            make_man_dtls = safe_get(box_details[0], ['woMakeManDtl'], default=[]) if box_details else []
            make_man_ppids = list(
                filter(
                    lambda ppid: ppid is not None,
                    map(lambda detail: safe_get(detail, ['asShippedPpid']), make_man_dtls)
                )
            )

            ppid_data = {
                "BasePPID": base_ppid,
                "AsShippedPPID": as_shipped_ppid,
                "MakeManPPIDs": make_man_ppids
            }

            shipment_boxes = safe_get(ASNDetailById[idx] if idx < len(ASNDetailById) else {}, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox'], default=[])
            total_box_count = len(list(filter(
                lambda box: safe_get(box, ['boxRef']) is not None,
                shipment_boxes
            )))

            total_gross_weight = sum(filter(
                lambda wt: wt is not None,
                map(lambda box: safe_get(box, ['boxGrossWt'], default=0), shipment_boxes)
            ))

            total_volumetric_weight = sum(filter(
                lambda wt: wt is not None,
                map(lambda box: safe_get(box, ['boxVolWt'], default=0), shipment_boxes)
            ))
            salesData = safe_get(salesorders[idx], ['salesOrder', 'salesOrderId'])
            
            if not any(row.get("Sales Order ID") == salesData for row in flat_list):
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
                    "WO ID": safe_get(salesorders[idx], ['workOrders', 0, 'woId']),
                    "System Qty": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'systemQty']),
                    "Ship By Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipByDate']),
                    "LOB": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
                    "Ship From Facility": safe_get(salesorders[idx], ['asnNumbers', 0, 'shipFrom']),
                    "Ship To Facility": safe_get(salesorders[idx], ['asnNumbers', 0, 'shipTo']),
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
                    "Actual Ship Mode":safe_get(ASNheaderByID[idx], ['shipMode']) if ASNheaderByID and 0 <= idx < len(ASNheaderByID) else "",
                    "First Leg Ship Mode":safe_get(ASNheaderByID[idx], ['shipMode']) if ASNheaderByID and 0 <= idx < len(ASNheaderByID) else "",
                    "ASN":safe_get(ASNheaderByID[idx], ['sourceManifestId']) if ASNheaderByID and 0 <= idx < len(ASNheaderByID) else "",
                    "Destination":safe_get(ASNheaderByID[idx], ['shipToVendorSiteId']) if ASNheaderByID and 0 <= idx < len(ASNheaderByID) else "",
                    "Manifest ID":safe_get(ASNheaderByID[idx], ['sourceManifestId']) if ASNheaderByID and 0 <= idx < len(ASNheaderByID) else "",
                    "Origin":safe_get(ASNheaderByID[idx], ['shipFromVendorSiteId']) if ASNheaderByID and 0 <= idx < len(ASNheaderByID) else "",
                    "Way Bill Number":safe_get(ASNheaderByID[idx], ['airwayBillNum']) if ASNheaderByID and 0 <= idx < len(ASNheaderByID) else "",
                    "Build Facility":"",
                    "Dell Blanket Po Num": safe_get(WorkOrderByID[idx], ['dellBlanketPoNum']) if WorkOrderByID and 0 <= idx < len(WorkOrderByID) else "",
                    "Has Software":has_software,
                    "Is Last Leg": safe_get(WorkOrderByID[idx], ['shipToFacility']) if WorkOrderByID and 0 <= idx < len(WorkOrderByID) else "",
                    "Make WoAck": MakeWoAckValue,                
                    "Mcid": McidValue,
                    "Ship From Mcid":safe_get(WorkOrderByID[idx], ['vendorSiteId']) if WorkOrderByID and 0 <= idx < len(WorkOrderByID) else "",
                    "Ship To Mcid":safe_get(WorkOrderByID[idx], ['shipToFacility']) if WorkOrderByID and 0 <= idx < len(WorkOrderByID) else "",
                    "Wo Otm Enable":safe_get(WorkOrderByID[idx], ['isOtmEnabled']) if WorkOrderByID and 0 <= idx < len(WorkOrderByID) else "",
                    "Work Order":safe_get(WorkOrderByID[idx], ['woId']) if WorkOrderByID and 0 <= idx < len(WorkOrderByID) else "",
                    "Wo Ship Mode":safe_get(WorkOrderByID[idx], ['shipMode']) if WorkOrderByID and 0 <= idx < len(WorkOrderByID) else "",
                    "Actual Ship Code": (
                        safe_get(
                            ASNDetailById[idx],
                            ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'shipviaCode']
                        )
                        if isinstance(ASNDetailById, list) and idx < len(ASNDetailById)
                        else ""
                    ),
                    "Order Vol Wt": (
                        safe_get(
                            ASNDetailById[idx],
                        ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'boxVolWt']
                        )
                        if isinstance(ASNDetailById, list) and idx < len(ASNDetailById)
                        else ""
                    ),
                    "PP ID": base_ppid,
                    "Svc Tag": (
                        safe_get(
                            ASNDetailById[idx],
                        ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails', 0, 'serviceTag']
                        )
                        if isinstance(ASNDetailById, list) and idx < len(ASNDetailById)
                        else ""
                    ),
                    "Target Delivery Date": (
                        safe_get(
                            ASNDetailById[idx],
                        ['manifestPallet', 0, 'woShipment', 0, 'estDeliveryDate']
                        )
                        if isinstance(ASNDetailById, list) and idx < len(ASNDetailById)
                        else ""
                    ),
                    "Total Box Count": total_box_count,
                    "Total Gross Weight": total_gross_weight,
                    "Total Volumetric Weight": total_volumetric_weight
                }

                flat_list.append(row)
