import json

flatt_list = []

# Prepare a mapping of Sales Order ID to related data
records_by_soid = {}

# Map Sales Orders
for so in salesorders:
    soid = safe_get(so, ['salesOrder', 'salesOrderId'])
    records_by_soid[soid] = {"salesorder": so}

# Map Fulfillments to Sales Orders
for ff in fulfillments_by_id:
    soid = safe_get(ff, ['salesOrderId'])
    if soid in records_by_soid:
        records_by_soid[soid]["fulfillment"] = ff

# Map Sales Headers
for sh in salesheaders_by_ids:
    soid = safe_get(sh, ['salesOrderId'])
    if soid in records_by_soid:
        records_by_soid[soid]["salesheader"] = sh

# Map Vendors (assume vendorSiteId relates to sales order or fulfillment)
vendor_map = {v.get("vendorSiteId"): v for v in VendormasterByVendor}

# Map ASN Headers
asnheader_map = {a.get("asnId"): a for a in ASNheaderByID}

# Map Work Orders
workorder_map = {w.get("woId"): w for w in WorkOrderByID}

# Map ASN Details
asn_detail_map = {ad.get("asnId"): ad for ad in ASNDetailById}

# Flatten each Sales Order record
for soid, record in records_by_soid.items():
    so = record.get("salesorder", {})
    ff = record.get("fulfillment", {})
    sh = record.get("salesheader", {})

    shipping_addr = pick_address_by_type(sh, "SHIPPING") if sh else {}
    billing_addr = pick_address_by_type(sh, "BILLING") if sh else {}

    # Sales Order basic info
    row = {
        "Sales Order ID": safe_get(so, ['salesOrder', 'salesOrderId']),
        "BUID": safe_get(so, ['salesOrder', 'buid']),
        "Region Code": safe_get(so, ['salesOrder', 'region']),
        "FO ID": safe_get(so, ['fulfillmentOrders', 0, 'foId']),
        "WO ID": safe_get(so, ['workOrders', 0, 'woId']),
        "Ship From Facility": safe_get(so, ['asnNumbers', 0, 'shipFrom']),
        "Ship To Facility": safe_get(so, ['asnNumbers', 0, 'shipTo']),
        "SN Number": safe_get(so, ['asnNumbers', 0, 'snNumber']),
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

    # Sales Header info
    if sh:
        row.update({
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
        })

    # Add Vendor info if applicable (example: take first vendor)
    if VendormasterByVendor:
        vendor = VendormasterByVendor[0]
        row["CFI Flag"] = safe_get(vendor, ['isCfi'], default="N")

    # Add Work Order info if applicable
    wo_id = safe_get(so, ['workOrders', 0, 'woId'])
    if wo_id and wo_id in workorder_map:
        wo = workorder_map[wo_id]
        wo_lines = safe_get(wo, ['woLines'], default=[])
        has_software = any(safe_get(line, ['woLineType'])=='SOFTWARE' for line in wo_lines)
        row.update({
            "Dell Blanket Po Num": safe_get(wo, ['dellBlanketPoNum'], default=""),
            "Has Software": has_software,
            "Work Order": wo_id,
            "Wo Ship Mode": safe_get(wo, ['shipMode'], default=""),
        })

    # Add ASN info if applicable
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

    # Add ASN Detail info if applicable
    if asn_id and asn_id in asn_detail_map:
        ad = asn_detail_map[asn_id]
        box_details = safe_get(ad, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails'], default=[])
        base_ppid = safe_get(box_details[0], ['basePpid']) if box_details else None
        row.update({
            "PP ID": base_ppid,
            "AsShippedPPID": safe_get(box_details[0], ['asShippedPpid']) if box_details else None,
            "Total Box Count": len(safe_get(ad, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox'], default=[]))
        })

    flatt_list.append(row)

# Output
print(json.dumps(flatt_list, indent=2))
exit()
