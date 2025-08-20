def OutputFormat(result_map, format_type=None, region=None):
    try:
        # ---------- helpers ----------
        def listify(x):
            if x is None:
                return []
            return x if isinstance(x, list) else [x]

        def first(lst, default=None):
            return lst[0] if isinstance(lst, list) and lst else default

        def pick_address_by_type(salesheader_entry: Dict, contact_type: str) -> Dict:
            addresses = listify(salesheader_entry.get("address", []))
            for addr in addresses:
                for c in listify(addr.get("contact")):
                    if isinstance(c, dict) and c.get("contactType") == contact_type:
                        return addr
            return {}

        def get_install_instruction2_id(fulfillment_entry: Dict) -> str:
            fulfills = listify(fulfillment_entry.get("fulfillments"))
            if not fulfills:
                return ""
            lines = listify(first(fulfills, {}).get("salesOrderLines"))
            for line in lines:
                for instr in listify(line.get("specialinstructions")):
                    if instr.get("specialInstructionType") == "INSTALL_INSTR2":
                        return str(instr.get("specialInstructionId", ""))
            return ""

        # ---------- extract payloads from result_map into *maps* keyed by IDs ----------
        sales_by_soid: Dict[str, Dict] = {}
        fulfill_by_id: Dict[str, Dict] = {}
        soheader_by_soid: Dict[str, Dict] = {}
        vendormaster_by_siteid: Dict[str, Dict] = {}
        asn_header_by_manifest: Dict[str, Dict] = {}
        workorder_by_woid: Dict[str, Dict] = {}
        asn_detail_by_manifest: Dict[str, Dict] = {}

        for item in result_map:
            data = item.get("data") if isinstance(item, dict) else None
            if not isinstance(data, dict):
                continue

            # getBySalesorderids
            if "getBySalesorderids" in data and isinstance(data["getBySalesorderids"], dict):
                for entry in listify(data["getBySalesorderids"].get("result")):
                    soid = safe_get(entry, ['salesOrder', 'salesOrderId'])
                    if soid:
                        sales_by_soid[soid] = entry

            # getFulfillmentsById
            if "getFulfillmentsById" in data and isinstance(data["getFulfillmentsById"], list):
                for entry in data["getFulfillmentsById"]:
                    # try to read fulfillmentId from nested structure
                    f_id = safe_get(entry, ['fulfillments', 0, 'fulfillmentId'])
                    if f_id:
                        fulfill_by_id[f_id] = entry

            # getSoheaderBySoids
            if "getSoheaderBySoids" in data and isinstance(data["getSoheaderBySoids"], list):
                for entry in data["getSoheaderBySoids"]:
                    # Some shapes: either have 'salesOrderId' or nested 'salesOrder'->'salesOrderId'
                    soid = entry.get('salesOrderId') or safe_get(entry, ['salesOrder', 'salesOrderId'])
                    if soid:
                        soheader_by_soid[soid] = entry

            # getVendormasterByVendorsiteid
            if "getVendormasterByVendorsiteid" in data and isinstance(data["getVendormasterByVendorsiteid"], list):
                for entry in data["getVendormasterByVendorsiteid"]:
                    # attempt multiple common keys for vendor site id
                    vsid = entry.get('vendorSiteId') or entry.get('vendorsiteid') or entry.get('vendorSite') or entry.get('vendor_site_id')
                    if vsid:
                        vendormaster_by_siteid[str(vsid)] = entry
                    # also index by uppercase (safety)
                    if vsid:
                        vendormaster_by_siteid[str(vsid).upper()] = entry

            # getAsnHeaderById
            if "getAsnHeaderById" in data and isinstance(data["getAsnHeaderById"], list):
                for entry in data["getAsnHeaderById"]:
                    manifest_id = entry.get('sourceManifestId')
                    if manifest_id:
                        asn_header_by_manifest[str(manifest_id)] = entry

            # getWorkOrderById
            if "getWorkOrderById" in data and isinstance(data["getWorkOrderById"], list):
                for entry in data["getWorkOrderById"]:
                    woid = entry.get('woId') or safe_get(entry, ['workOrderId'])
                    if woid:
                        workorder_by_woid[str(woid)] = entry

            # getAsnDetailById (dict)
            if "getAsnDetailById" in data and isinstance(data["getAsnDetailById"], dict):
                asn_detail = data["getAsnDetailById"]
                # Try to find sourceManifestId in a few places
                manifest_id = (
                    asn_detail.get('sourceManifestId')
                    or safe_get(asn_detail, ['manifestPallet', 0, 'sourceManifestId'])
                    or safe_get(asn_detail, ['manifestPallet', 0, 'woShipment', 0, 'sourceManifestId'])
                )
                if manifest_id:
                    asn_detail_by_manifest[str(manifest_id)] = asn_detail

        # ---------- synthesize flat rows keyed by Sales Order ----------
        flat_list = []

        for soid, so_entry in sales_by_soid.items():
            # Sales header (addresses etc.)
            so_header = soheader_by_soid.get(soid, {})

            shipping_addr = pick_address_by_type(so_header, "SHIPPING")
            billing_addr  = pick_address_by_type(so_header, "BILLING")

            ship_first = shipping_addr.get("firstName", "") if shipping_addr else ""
            ship_last = shipping_addr.get("lastName", "") if shipping_addr else ""
            shipping_contact_name = (f"{ship_first} {ship_last}").strip()

            # IDs from sales record
            fulfillment_id = safe_get(so_entry, ['fulfillment', 0, 'fulfillmentId'])
            fo_id          = safe_get(so_entry, ['fulfillmentOrders', 0, 'foId'])
            wo_id          = safe_get(so_entry, ['workOrders', 0, 'woId'])
            buid           = safe_get(so_entry, ['salesOrder', 'buid'])
            region_code    = safe_get(so_entry, ['salesOrder', 'region'])

            # Fulfillment (detailed)
            full_entry = fulfill_by_id.get(fulfillment_id, {}) if fulfillment_id else {}

            # Work order details
            wo_entry = workorder_by_woid.get(str(wo_id), {}) if wo_id else {}

            # ASN header/details by manifest
            manifest_id   = safe_get(so_entry, ['asnNumbers', 0, 'sourceManifestId'])
            asn_header    = asn_header_by_manifest.get(str(manifest_id), {}) if manifest_id else {}
            asn_detail    = asn_detail_by_manifest.get(str(manifest_id), {}) if manifest_id else {}

            # Vendor master (by shipToVendorSiteId from ASN header, if available)
            vendor_site_id = asn_header.get('shipToVendorSiteId') or asn_header.get('shipToVendorSiteID')
            vend_entry = vendormaster_by_siteid.get(str(vendor_site_id), {}) if vendor_site_id else {}
            if vendor_site_id and not vend_entry:
                vend_entry = vendormaster_by_siteid.get(str(vendor_site_id).upper(), {})

            # Has software?
            wo_lines = listify(wo_entry.get('woLines'))
            has_software = any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in wo_lines)

            # MakeWoAck
            make_wo_ack = (
                "True"
                if wo_entry.get('woType') == 'MAKE' and any(
                    str(s.get('channelStatusCode')) == '3000'
                    for s in listify(wo_entry.get('woStatusList'))
                )
                else "False"
            )

            # Mcid
            merge_fac = safe_get(wo_entry, ['woShipInstr', 0, 'mergeFacility'])
            carrier_hub = safe_get(wo_entry, ['woShipInstr', 0, 'carrierHubCode'])
            mcid_value = merge_fac if merge_fac else (carrier_hub or "")

            # ASN detail-derived
            box_details = safe_get(asn_detail, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails'], default=[])
            base_ppid = safe_get(first(box_details, {}), ['basePpid'])
            as_shipped_ppid = safe_get(first(box_details, {}), ['asShippedPpid'])

            make_man_dtls = safe_get(first(box_details, {}), ['woMakeManDtl'], default=[]) if box_details else []
            make_man_ppids = [
                p for p in (safe_get(d, ['asShippedPpid']) for d in make_man_dtls)
                if p is not None
            ]

            ppid_data = {
                "BasePPID": base_ppid,
                "AsShippedPPID": as_shipped_ppid,
                "MakeManPPIDs": make_man_ppids
            }

            shipment_boxes = safe_get(asn_detail, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox'], default=[])
            total_box_count = len([b for b in shipment_boxes if safe_get(b, ['boxRef']) is not None])
            total_gross_weight = sum(
                wt for wt in (safe_get(b, ['boxGrossWt'], default=0) for b in shipment_boxes) if wt is not None
            )
            total_volumetric_weight = sum(
                wt for wt in (safe_get(b, ['boxVolWt'], default=0) for b in shipment_boxes) if wt is not None
            )

            # Address phones
            ship_phone = ""
            if shipping_addr:
                phones = listify(shipping_addr.get("phone"))
                ship_phone = first(phones, {}).get("phoneNumber", "") if phones else ""

            # InstallInstruction2 (from fulfillment payload if available)
            install_instr2 = get_install_instruction2_id(full_entry)

            # PP/IP/MN/SC dates — based on the *first* sostatus element
            so_status = safe_get(full_entry, ['fulfillments', 0, 'sostatus', 0], default={})
            ss_code = so_status.get('sourceSystemStsCode')
            ss_date = dateFormation(so_status.get('statusDate'))

            pp_date = ss_date if ss_code == "PP" else ""
            ip_date = ss_date if ss_code == "IP" else ""
            mn_date = ss_date if ss_code == "MN" else ""
            sc_date = ss_date if ss_code == "SC" else ""

            # CFI flag (default "N" if missing)
            cfi_flag = vend_entry.get('isCfi', "N")

            # Build the row
            row = {
                "Fulfillment ID": fulfillment_id,
                "BUID": buid,
                "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                "InstallInstruction2": install_instr2,
                "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                "ShippingContactName": shipping_contact_name,
                "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                "ShipToPhone": ship_phone,
                "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",

                "PP Date": pp_date,
                "IP Date": ip_date,
                "MN Date": mn_date,
                "SC Date": sc_date,

                "CFI Flag": cfi_flag,
                "Agreement ID": safe_get(so_header, ['agreementId']),
                "Amount": safe_get(so_header, ['totalPrice']),
                "Currency Code": safe_get(so_header, ['currency']),
                "Customer Po Number": safe_get(so_header, ['poNumber']),
                "Dp ID": safe_get(so_header, ['dpid']),
                "Location Number": safe_get(so_header, ['locationNum']),
                "Order Age": safe_get(so_header, ['orderDate']),
                "Order Amount usd": safe_get(so_header, ['rateUsdTransactional']),
                "Order Update Date": safe_get(so_header, ['updateDate']),
                "Rate Usd Transactional": safe_get(so_header, ['rateUsdTransactional']),
                "Sales Rep Name": safe_get(so_header, ['salesrep', 0, 'salesRepName']),
                "Shipping Country": safe_get(so_header, ['address', 0, 'country']),

                "Source System Status": safe_get(full_entry, ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
                "Tie Number": safe_get(full_entry, ['fulfillments', 0, 'salesOrderLines', 0, 'soLineNum']),
                "Si Number": safe_get(full_entry, ['fulfillments', 0, 'salesOrderLines', 0, 'siNumber']),
                "Req Ship Code": safe_get(full_entry, ['fulfillments', 0, 'shipCode']),
                "Reassigned Ip Date": safe_get(full_entry, ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
                "RDD": safe_get(full_entry, ['fulfillments', 0, 'revisedDeliveryDate']),
                "Product Lob": safe_get(full_entry, ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
                "Payment Term Code": safe_get(full_entry, ['fulfillments', 0, 'paymentTerm']),
                "Ofs Status Code": safe_get(full_entry, ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
                "Ofs Status": safe_get(full_entry, ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
                "Fulfillment Status": safe_get(full_entry, ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
                "DomsStatus": safe_get(full_entry, ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),

                "Sales Order ID": soid,
                "Region Code": region_code,
                "FO ID": fo_id,
                "WO ID": wo_id,
                "System Qty": safe_get(full_entry, ['fulfillments', 0, 'systemQty']),
                "Ship By Date": safe_get(full_entry, ['fulfillments', 0, 'shipByDate']),
                "LOB": safe_get(full_entry, ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
                "Ship From Facility": safe_get(so_entry, ['asnNumbers', 0, 'shipFrom']),
                "Ship To Facility": safe_get(so_entry, ['asnNumbers', 0, 'shipTo']),
                "Facility": safe_get(full_entry, ['fulfillments', 0, 'salesOrderLines', 0, 'facility']),
                "SN Number": safe_get(so_entry, ['asnNumbers', 0, 'snNumber']),
                "Tax Regstrn Num": safe_get(full_entry, ['fulfillments', 0, 'address', 0, 'taxRegstrnNum']),
                "State Code": safe_get(full_entry, ['fulfillments', 0, 'address', 0, 'stateCode']),
                "City Code": safe_get(full_entry, ['fulfillments', 0, 'address', 0, 'cityCode']),
                "Customer Num": safe_get(full_entry, ['fulfillments', 0, 'address', 0, 'customerNum']),
                "Customer Name Ext": safe_get(full_entry, ['fulfillments', 0, 'address', 0, 'customerNameExt']),
                "Country": safe_get(full_entry, ['fulfillments', 0, 'address', 0, 'country']),
                "Create Date": dateFormation(dateFormation(safe_get(so_header, ['createDate']))),
                "Ship Code": safe_get(full_entry, ['fulfillments', 0, 'shipCode']),
                "Must Arrive By Date": dateFormation(safe_get(full_entry, ['fulfillments', 0, 'mustArriveByDate'])),
                "Update Date": dateFormation(safe_get(full_entry, ['fulfillments', 0, 'updateDate'])),
                "Merge Type": safe_get(full_entry, ['fulfillments', 0, 'mergeType']),
                "Manifest Date": dateFormation(safe_get(full_entry, ['fulfillments', 0, 'manifestDate'])),
                "Revised Delivery Date": dateFormation(safe_get(full_entry, ['fulfillments', 0, 'revisedDeliveryDate'])),
                "Delivery City": safe_get(full_entry, ['fulfillments', 0, 'deliveryCity']),
                "Source System ID": safe_get(full_entry, ['sourceSystemId']),
                "OIC ID": safe_get(full_entry, ['fulfillments', 0, 'oicId']),
                "Order Date": dateFormation(safe_get(so_header, ['orderDate'])),

                "Actual Ship Mode": safe_get(asn_header, ['shipMode']),
                "First Leg Ship Mode": safe_get(asn_header, ['shipMode']),
                "ASN": safe_get(asn_header, ['sourceManifestId']),
                "Destination": safe_get(asn_header, ['shipToVendorSiteId']),
                "Manifest ID": safe_get(asn_header, ['sourceManifestId']),
                "Origin": safe_get(asn_header, ['shipFromVendorSiteId']),
                "Way Bill Number": safe_get(asn_header, ['airwayBillNum']),

                "Build Facility": "",
                "Dell Blanket Po Num": safe_get(wo_entry, ['dellBlanketPoNum']),
                "Has Software": has_software,
                "Is Last Leg": safe_get(wo_entry, ['shipToFacility']),
                "Make Wo Ack": make_wo_ack,
                "Mcid": mcid_value,
                "Ship From Mcid": safe_get(wo_entry, ['vendorSiteId']),
                "Ship To Mcid": safe_get(wo_entry, ['shipToFacility']),
                "Wo Otm Enable": safe_get(wo_entry, ['isOtmEnabled']),
                "Work Order": safe_get(wo_entry, ['woId']),
                "Wo Ship Mode": safe_get(wo_entry, ['shipMode']),

                "Actual Ship Code": safe_get(asn_detail, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'shipviaCode']),
                "Order Vol Wt": safe_get(asn_detail, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'boxVolWt']),
                "PP ID": ppid_data.get("BasePPID"),
                "Svc Tag": safe_get(asn_detail, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails', 0, 'serviceTag']),
                "Target Delivery Date": safe_get(asn_detail, ['manifestPallet', 0, 'woShipment', 0, 'estDeliveryDate']),
                "Total Box Count": total_box_count,
                "Total Gross Weight": total_gross_weight,
                "Total Volumetric Weight": total_volumetric_weight
            }

            # ensure uniqueness in case of dup SOIDs in payload
            if not any(r.get("Sales Order ID") == soid for r in flat_list):
                flat_list.append(row)

        # ---------- final formatting ----------
        if not flat_list:
            return {"Error Message": "No Data Found"}

        if format_type == "export":
            return flat_list

        if format_type == "grid":
            desired_order = [
                "Fulfillment ID","BUID",
                "BillingCustomerName","CustomerName","InstallInstruction2","ShippingCityCode",
                "ShippingContactName","ShippingCustName","ShippingStateCode",
                "ShipToAddress1","ShipToAddress2","ShipToCompany","ShipToPhone","ShipToPostal",
                "PP Date","IP Date","MN Date","SC Date","CFI Flag","Agreement ID","Amount","Currency Code",
                "Customer Po Number","Dp ID","Location Number","Order Age","Order Amount usd","Order Update Date",
                "Rate Usd Transactional","Sales Rep Name","Shipping Country","Source System Status","Tie Number",
                "Si Number","Req Ship Code","Reassigned Ip Date","RDD","Product Lob","Payment Term Code","Ofs Status Code",
                "Ofs Status","Fulfillment Status","DomsStatus",
                "Sales Order ID","Region Code","FO ID","WO ID","System Qty","Ship By Date","LOB",
                "Ship From Facility","Ship To Facility","Facility","SN Number","Tax Regstrn Num","State Code","City Code",
                "Customer Num","Customer Name Ext","Country","Create Date","Ship Code","Must Arrive By Date","Update Date",
                "Merge Type","Manifest Date","Revised Delivery Date","Delivery City","Source System ID","OIC ID","Order Date",
                "Actual Ship Mode","First Leg Ship Mode","ASN","Destination","Manifest ID","Origin","Way Bill Number",
                "Build Facility","Dell Blanket Po Num","Has Software","Is Last Leg","Make Wo Ack","Mcid","Ship From Mcid",
                "Ship To Mcid","Wo Otm Enable","Work Order","Wo Ship Mode",
                "Actual Ship Code","Order Vol Wt","PP ID","Svc Tag","Target Delivery Date","Total Box Count","Total Gross Weight",
                "Total Volumetric Weight"
            ]

            rows = []
            for item in flat_list:
                reordered_values = [item.get(key, "") for key in desired_order]
                row = {"columns": [{"value": ("" if v is None else v)} for v in reordered_values]}
                rows.append(row)
            table_grid_output = tablestructural(rows, region) if rows else []
            return table_grid_output

        return {"error": "Format type must be either 'grid' or 'export'"}

    except Exception as e:
        print(f"[ERROR] OutputFormat failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}
