# Fetch Sales Order
salesorder_resp = post_api(FID, fetch_salesorder_query(salesorder_id))
get_by_salesorderids = salesorder_resp['data'].get('getBySalesorderids')
if not get_by_salesorderids or not get_by_salesorderids.get('result'):
    raise ValueError(f"No result found for Sales Order ID: {salesorder_id}")

result = get_by_salesorderids['result'][0]
combined_data['data']['getBySalesorderids'] = get_by_salesorderids
