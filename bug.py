def combined_salesorder_fetch(so_id):
    combined_salesorder_data = {'data': {}}

    # Prepare input variables
    soi = {"salesorderIds": [so_id]}

    # First Query - getSoheaderBySoids
    try:
        soaorder_query = fetch_soaorder_query()
        soaorder = post_api(URL=SOPATH, query=soaorder_query, variables=soi)
        soheaders = soaorder.get('data', {}).get('getSoheaderBySoids', [])

        # Ensure it's a list
        if isinstance(soheaders, dict):
            soheaders = [soheaders]
        elif not isinstance(soheaders, list):
            soheaders = []

        combined_salesorder_data['data']['getSoheaderBySoids'] = soheaders
    except Exception as e:
        print(f"[ERROR] getSoheaderBySoids failed for {so_id}: {e}")
        combined_salesorder_data['data']['getSoheaderBySoids'] = []

    # Second Query - getBySalesorderids
    try:
        salesorder_query = fetch_salesorder_query(so_id)
        salesorder = post_api(URL=FID, query=salesorder_query, variables=None)
        soorders = salesorder.get('data', {}).get('getBySalesorderids', [])

        # Ensure it's a list
        if isinstance(soorders, dict):
            soorders = [soorders]
        elif not isinstance(soorders, list):
            soorders = []

        combined_salesorder_data['data']['getBySalesorderids'] = soorders
    except Exception as e:
        print(f"[ERROR] getBySalesorderids failed for {so_id}: {e}")
        combined_salesorder_data['data']['getBySalesorderids'] = []

    return combined_salesorder_data
