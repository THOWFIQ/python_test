def combined_salesorder_fetch(so_id, region, filters, CollectedValue):
    try:
        path = getPath(region)

        soi = {"salesorderIds": [so_id]}
        if so_id is not None:
            soaorder_query = fetch_soaorder_query()
            soaorder = post_api(URL=path['SOPATH'], query=soaorder_query, variables=soi)
            if soaorder and soaorder.get('data'):
                combined_salesorder_data['data']['getSoheaderBySoids'] = soaorder['data'].get('getSoheaderBySoids', [])

            salesorder_query = fetch_salesorder_query(so_id)
            salesorder = post_api(URL=path['FID'], query=salesorder_query, variables=None)
            if salesorder and salesorder.get('data') and 'getBySalesorderids' in salesorder['data']:
                combined_salesorder_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']


        return combined_salesorder_data
    except Exception as e:
        print(f"Error in combined_salesorder_fetch: {e}")
        return {}

        this above is my function i'm getting following error 

        Error in combined_salesorder_fetch: 'NoneType' object is not subscriptable
