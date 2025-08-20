def OutputFormat(result_map, format_type=None, region=None):
    try:
        # Build dictionaries keyed by IDs for safe lookup
        salesorders = {
            item["data"]["getBySalesorderids"]["result"][0]["salesorderid"]: 
            item["data"]["getBySalesorderids"]["result"][0]
            for item in result_map if "getBySalesorderids" in item["data"]
        }

        fulfillments = {
            f["salesorderid"]: f
            for item in result_map if "getFulFillmentBySalesorderids" in item["data"]
            for f in item["data"]["getFulFillmentBySalesorderids"]["result"]
        }

        salesheaders = {
            h["salesorderid"]: h
            for item in result_map if "getSalesHeaderBySalesorderids" in item["data"]
            for h in item["data"]["getSalesHeaderBySalesorderids"]["result"]
        }

        vendors = {
            v["vendorid"]: v
            for item in result_map if "getVendorMasterByVendorids" in item["data"]
            for v in item["data"]["getVendorMasterByVendorids"]["result"]
        }

        asnheaders = {
            a["salesorderid"]: a
            for item in result_map if "getASNHeaderBySalesorderids" in item["data"]
            for a in item["data"]["getASNHeaderBySalesorderids"]["result"]
        }

        works = {
            w["salesorderid"]: w
            for item in result_map if "getWorkOrderBySalesorderids" in item["data"]
            for w in item["data"]["getWorkOrderBySalesorderids"]["result"]
        }

        # Final merged output
        output = []
        for so_id, so in salesorders.items():
            merged = {
                "salesorder": so,
                "fulfillment": fulfillments.get(so_id),
                "salesheader": salesheaders.get(so_id),
                "asnheader": asnheaders.get(so_id),
                "work": works.get(so_id)
            }

            # Add vendor details if available
            vendor_id = so.get("vendorid")
            if vendor_id and vendor_id in vendors:
                merged["vendor"] = vendors[vendor_id]

            output.append(merged)

        return output

    except Exception as e:
        print(f"[ERROR] OutputFormat failed: {str(e)}")
        return []
