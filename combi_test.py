import json

flatt_list = []

# Create lookup maps for easy access
workorder_map = {w.get("woId"): w for w in WorkOrderByID}
asnheader_map = {a.get("asnId"): a for a in ASNheaderByID}
asn_detail_map = {ad.get("asnId"): ad for ad in ASNDetailById}
vendor_map = {v.get("vendorSiteId"): v for v in VendormasterByVendor}

for so in salesorders:
    soid = safe_get(so, ['salesOrder', 'salesOrderId'])
    ff = next((f for f in fulfillments_by_id if safe_get(f, ['salesOrderId'])==soid), {})
    sh = next((s for s in salesheaders_by_ids if safe_get(s, ['salesOrderId'])==soid), {})

    shipping_addr = pick_address_by_type(sh, "SHIPPING") if sh else {}
    billing_addr = pick_address_by_type(sh, "BILLING") if sh else {}

    row = {
        # Sales Order info
        "Sales Order ID": soid,
        "BUID": safe_get(so, ['salesOrder', 'buid']),
        "Region Code": safe_get(so, ['salesOrder', 'region']),
        "FO ID": safe_get(so, ['fulfillmentOrders', 0, 'foId']),
        "WO ID": safe_get(so, ['workOrders', 0, 'woId']),
        "Ship From Facility": safe_get(so, ['asnNumbers', 0, 'shipFrom']),
        "Ship To Facility": safe_get(so, ['asnNumbers', 0, 'shipTo']),
        "SN Number": safe_get(so, ['asnNumbers', 0, 'snNumber']),

        # Sales Header info
        "Agreement ID": safe_get(sh, ['agreementId']),
        "Amount": safe_get(sh, ['totalPrice']),
        "Currency Code": safe_get(sh, ['currency']),
        "Customer Po Number": safe_get(sh, ['poNumber']),
        "Dp ID": safe_get(sh, ['dpid']),
        "Location Number": safe_get(sh, ['locationNum']),
        "Order Age": safe_get(sh, ['orderDate']),
        "Order Amount usd": safe_get(sh, ['rateUsdTransactional']),
        "Order Update Date": safe_get(sh, ['updateDate']),
        "Sales Rep Name": safe_get(sh, ['salesrep', 0, 'salesRepName']),
        "Shipping Country": safe_get(shipping_addr, ['country']),
        "Create Date": dateFormation(safe_get(sh, ['createDate'])),
        "Order Date": dateFormation(safe_get(sh, ['orderDate'])),
    }

    # Fulfillment info
    if ff:
        sostatus = safe_get(ff, ['fulfillments', 0, 'sostatus', 0], default={})
        row.update({
            "PP Date": dateFormation(sostatus['statusDate']) if sostatus.get('sourceSystemStsCode')=="PP" else "",
            "IP Date": dateFormation(sostatus['statusDate']) if sostatus.get('sourceSystemStsCode')=="IP" else "",
            "MN Date": dateFormation(sostatus['statusDate']) if sostatus.get('sourceSystemStsCode')=="MN" else "",
            "SC Date": dateFormation(sostatus['statusDate']) if sostatus.get('sourceSystemStsCode')=="SC" else "",
            "Source System Status": sostatus.get('sourceSystemStsCode'),
            "Tie Number": safe_get(ff, ['fulfillments', 0, 'salesOrderLines', 0, 'soLineNum']),
            "Si Number": safe_get(ff, ['fulfillments', 0, 'salesOrderLines', 0, 'siNumber']),
            "Req Ship Code": safe_get(ff, ['fulfillments', 0, 'shipCode']),
            "RDD": safe_get(ff, ['fulfillments', 0, 'revisedDeliveryDate']),
            "Product Lob": safe_get(ff, ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
            "Payment Term Code": safe_get(ff, ['fulfillments', 0, 'paymentTerm']),
            "System Qty": safe_get(ff, ['fulfillments', 0, 'systemQty']),
            "Ship By Date": safe_get(ff, ['fulfillments', 0, 'shipByDate']),
            "Facility": safe_get(ff, ['fulfillments', 0, 'salesOrderLines', 0, 'facility']),
            "Tax Regstrn Num": safe_get(ff, ['fulfillments', 0, 'address', 0, 'taxRegstrnNum']),
            "State Code": safe_get(ff, ['fulfillments', 0, 'address', 0, 'stateCode']),
            "City Code": safe_get(ff, ['fulfillments', 0, 'address', 0, 'cityCode']),
            "Customer Num": safe_get(ff, ['fulfillments', 0, 'address', 0, 'customerNum']),
            "Customer Name Ext": safe_get(ff, ['fulfillments', 0, 'address', 0, 'customerNameExt']),
            "Country": safe_get(ff, ['fulfillments', 0, 'address', 0, 'country']),
            "Must Arrive By Date": dateFormation(safe_get(ff, ['fulfillments', 0, 'mustArriveByDate'])),
            "Update Date": dateFormation(safe_get(ff, ['fulfillments', 0, 'updateDate'])),
            "Manifest Date": dateFormation(safe_get(ff, ['fulfillments', 0, 'manifestDate'])),
        })

    # Work Order info
    wo_id = safe_get(so, ['workOrders', 0, 'woId'])
    if wo_id and wo_id in workorder_map:
        wo = workorder_map[wo_id]
        wo_lines = safe_get(wo, ['woLines'], default=[])
        has_software = any(safe_get(line, ['woLineType'])=='SOFTWARE' for line in wo_lines)
        MakeWoAckValue = "True" if wo.get('woType')=='MAKE' and any(str(status.get('channelStatusCode'))=='3000' for status in wo.get('woStatusList', [])) else "False"
        McidValue = wo.get('woShipInstr', [{}])[0].get('mergeFacility') or wo.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
        row.update({
            "Dell Blanket Po Num": safe_get(wo, ['dellBlanketPoNum'], ""),
            "Has Software": has_software,
            "Make WoAck": MakeWoAckValue,
            "Mcid": McidValue,
            "Work Order": wo_id,
            "Wo Ship Mode": safe_get(wo, ['shipMode'], ""),
        })

    # ASN Header info
    asn_id = safe_get(ff, ['asnNumbers', 0, 'asnId'])
    if asn_id and asn_id in asnheader_map:
        asn = asnheader_map[asn_id]
        row.update({
            "ASN": safe_get(asn, ['sourceManifestId']),
            "Actual Ship Mode": safe_get(asn, ['shipMode']),
            "Destination": safe_get(asn, ['shipToVendorSiteId']),
            "Origin": safe_get(asn, ['shipFromVendorSiteId']),
            "Way Bill Number": safe_get(asn, ['airwayBillNum']),
        })

    # ASN Detail info
    if asn_id and asn_id in asn_detail_map:
        ad = asn_detail_map[asn_id]
        shipment_boxes = safe_get(ad, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox'], default=[])
        total_box_count = len(shipment_boxes)
        total_gross_weight = sum(safe_get(box, ['boxGrossWt'], 0) for box in shipment_boxes)
        total_volumetric_weight = sum(safe_get(box, ['boxVolWt'], 0) for box in shipment_boxes)

        # PPIDs
        box_details = safe_get(shipment_boxes[0] if shipment_boxes else {}, ['woShipmentBoxDetails'], default=[])
        base_ppid = safe_get(box_details[0], ['basePpid']) if box_details else ""
        as_shipped_ppid = safe_get(box_details[0], ['asShippedPpid']) if box_details else ""
        make_man_dtls = safe_get(box_details[0], ['woMakeManDtl'], default=[]) if box_details else []
        make_man_ppids = [safe_get(d, ['asShippedPpid']) for d in make_man_dtls if safe_get(d, ['asShippedPpid'])]

        svc_tag = safe_get(box_details[0], ['serviceTag']) if box_details else ""
        target_delivery_date = safe_get(shipment_boxes[0], ['estDeliveryDate']) if shipment_boxes else ""
        actual_ship_code = safe_get(shipment_boxes[0], ['shipviaCode']) if shipment_boxes else ""
        order_vol_wt = safe_get(shipment_boxes[0], ['boxVolWt']) if shipment_boxes else ""

        row.update({
            "BasePPID": base_ppid,
            "AsShippedPPID": as_shipped_ppid,
            "MakeManPPIDs": make_man_ppids,
            "Svc Tag": svc_tag,
            "Target Delivery Date": target_delivery_date,
            "Actual Ship Code": actual_ship_code,
            "Order Vol Wt": order_vol_wt,
            "Total Box Count": total_box_count,
            "Total Gross Weight": total_gross_weight,
            "Total Volumetric Weight": total_volumetric_weight,
        })

    # Vendor info
    if VendormasterByVendor:
        vendor = VendormasterByVendor[0]
        row["CFI Flag"] = safe_get(vendor, ['isCfi'], "N")

    flatt_list.append(row)

# Output final flattened list
print(json.dumps(flatt_list, indent=2))
exit()
