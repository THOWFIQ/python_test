if "Sales_Order_id" in filters:
    url = path['FID']
    sales_ids = list(map(str.strip, filters["Sales_Order_id"].split(",")))
    for batch in chunk_list(sales_ids, 50):
        payload = {
            "query": fetch_salesorderf_query(json.dumps(batch))
        }
        response = requests.post(url, json=payload, verify=False)
        data = response.json()
        if "errors" in data:
            return jsonify({"error": data["errors"]}), 500

        result = data.get("data", {}).get("getBySalesorderids", {})
        for entry in result.get("result", []):
            record = SalesRecord(
                asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])],
                fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
                fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
            )
            records.append(record)

            # 🔹 New Step 1: Extract shipFromVendorId & sourceManifestId
            ship_from_vendor_id = entry.get("shipFromVendorId")
            source_manifest_id = entry.get("sourceManifestId")

            if ship_from_vendor_id and source_manifest_id:
                # 🔹 New Step 2: Call ASNODM
                asn_url = path['ASNODM']
                asn_payload = {
                    "query": getAsnHeaderById(
                        json.dumps(ship_from_vendor_id),
                        json.dumps(source_manifest_id)
                    )
                }
                asn_response = requests.post(asn_url, json=asn_payload, verify=False)
                asn_data = asn_response.json()

                # Extract shipToVendorId
                ship_to_vendor_id = (
                    asn_data.get("data", {})
                           .get("getAsnHeaderById", {})
                           .get("shipToVendorId")
                )

                if ship_to_vendor_id:
                    # 🔹 New Step 3: Call Vendor Master
                    vendor_url = path['VENDOR']
                    vendor_payload = {
                        "query": getVendormasterByVendorid(json.dumps(ship_to_vendor_id))
                    }
                    vendor_response = requests.post(vendor_url, json=vendor_payload, verify=False)
                    vendor_data = vendor_response.json()

                    # Append vendor data to records
                    records.append(vendor_data)



once  following result get mean 

if obj.salesOrderId and obj.salesOrderId.salesOrderId:
            graphql_request.append({
                "url": path['FID'],
                "query": fetch_salesorder_query(json.dumps(obj.salesOrderId.salesOrderId))
            })
            print(f"[{countReqNo}] Sales Order: {obj.salesOrderId.salesOrderId}")

get this response and collect two feilds (shipFromVendorId, sourceManifestId)

then call 

	URL : path['ASNODM']

    payload = {
        "query": getAsnHeaderById(json.dumps(obj.salesOrderId.shipFromVendorId),json.dumps(obj.salesOrderId.sourceManifestId))
    }
    response = requests.post(url, json=payload, verify=False)
    data = response.json()

then collect this field  -> shipToVendorId

then call 

        URL : path['VENDOR']

        payload = {
            "query": getVendormasterByVendorid(json.dumps(obj.salesOrderId.shipToVendorId))
        }
        response = requests.post(url, json=payload, verify=False)
        data = response.json()

records.append(data)
