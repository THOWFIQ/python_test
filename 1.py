# Get current vendorId for matching
    vendor_id = safe_get(salesheaders_by_ids[idx], ['vendorId'])

    # Find the vendor record in VendormasterByVendor
    vendor_data = next((v for v in VendormasterByVendor if v.get('vendorId') == vendor_id), None)
