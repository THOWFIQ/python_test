from concurrent.futures import ThreadPoolExecutor
 
q=[[11,37,53,81,5],
[21,34,56,82,6],
[90,38,89,84,7],
[33,32,53,54,8],
[93,72,13,14,33]]


 
def task(n):
    print (q[n])
    return q[n]
 
with ThreadPoolExecutor(max_workers=3) as executor:
    # results = list(executor.map(task, range(5)))
    futures = [executor.submit(task, i) for i in range(5)]     
    results = [future.result() for future in futures]
 
print(results)


OrderDate_Response = combined_OrderDate_fetch(from_date, to_date, region, filters)
                resultData = OrderDate_Response.get('data', {}).get('getOrdersByDate', {}).get('result', [])
                
                SalesOrderIDs = [item.get("salesOrderId") for item in resultData if item.get("salesOrderId")]

                print("Total Sales Order IDs:", len(SalesOrderIDs))

def combined_salesorder_fetch(so_id, region, filters):
    start_time = time.time()
    combined_salesorder_data = {'data': {}}
    try:
        path = getPath(region)

        soi = json.dumps(so_id)
       
        if so_id is not None:
            salesorder_query = fetch_salesorder_query(soi)
            salesorder = post_api(URL=path['FID'], query=salesorder_query, variables=None)

            if salesorder and salesorder.get('data') and 'getBySalesorderids' in salesorder['data']:
                combined_salesorder_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"Sales Orer Function took {elapsed_time:.2f} seconds to complete.")
        
        return combined_salesorder_data
    except Exception as e:
        print(f"Error in combined_salesorder_fetch: {e}")
        return {}

def combined_fulfillment_fetch(fulfillment_id, region, filters):
    start_time = time.time()
    combined_fullfillment_data = {'data': {}}
    try:
        path = getPath(region)        
               
        fetchfillmentids_query = fetch_getByFulfillmentids_query(fulfillment_id)
        fetchfillmentids_data = post_api(URL=path['FID'], query=fetchfillmentids_query, variables=None)
       
        if fetchfillmentids_data and fetchfillmentids_data.get('data'):
            combined_fullfillment_data['data']['getByFulfillmentids'] = fetchfillmentids_data['data'].get('getByFulfillmentids', {})

        sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
        sofulfillment_data = post_api(URL=path['SOPATH'], query=sofulfillment_query, variables=None)
        
        if sofulfillment_data and sofulfillment_data.get('data'):
            combined_fullfillment_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})

        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"Fullfillment Function took {elapsed_time:.2f} seconds to complete.")
        
        return combined_fullfillment_data

    except Exception as e:
        print(f"Error in combined_fulfillment_fetch: {e}")
        return {}

def combined_OrderDate_fetch(orderFromDate, orderToDate, region, filters):
    start_time = time.time()
    combined_orderDate_data = {'data': {}}
    try:
        path = getPath(region)
        orderDate_query = fetch_getOrderDate_query(orderFromDate, orderToDate)

        orderDate_data = post_api(URL=path['SOPATH'], query=orderDate_query, variables=None)

        if orderDate_data and orderDate_data.get('data'):
            combined_orderDate_data['data']['getOrdersByDate'] = orderDate_data['data'].get('getOrdersByDate', {})
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"order date Function took {elapsed_time:.2f} seconds to complete.")
        return combined_orderDate_data

    except Exception as e:
        print(f"[ERROR] Error in combined_orderDate_data_fetch: {e}")
        return {}

def extract_fulfillment_ids_with_map(all_data):
    results = chain.from_iterable(
        map(lambda entry: entry.get("data", {}).get("getBySalesorderids", {}).get("result", []), all_data)
    )

    fulfillments = chain.from_iterable(
        map(lambda result: result.get("fulfillment", []) if isinstance(result.get("fulfillment"), list) else [], results)
    )

    fulfillment_ids = list(
        map(lambda f: f.get("fulfillmentId"), filter(lambda f: f.get("fulfillmentId"), fulfillments))
    )

    return fulfillment_ids

def run_multithread_batches(fetch_func, ids, region, filters, batch_size=50, max_workers=30, delay_between_batches=0.5):
    start_time = time.time()
    all_results = []
    
    def wrapper(batch_ids):
        results = []
        
        with ThreadPoolExecutor(max_workers=min(len(batch_ids), max_workers)) as executor:
            futures = [executor.submit(fetch_func, batch_ids, region, filters)]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                except Exception as e:
                    print(f"Error fetching ID in batch: {e}")
        return results

    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        print(f"\nProcessing batch {i//batch_size + 1} ({len(batch)} items)...")

        try:
            batch_results = wrapper(batch)
            all_results.extend(batch_results)
        except Exception as e:
            print(f"Error in batch {i//batch_size + 1}: {e}")

        time.sleep(delay_between_batches)

    end_time = time.time()
    print(f"Thread Function took {end_time - start_time:.2f} seconds to complete.")
    return all_results

def thread_fetch_and_store(fetch_func, id_list, region, filters, key_name, result_map, max_workers=50, batch_size=30):
    results = []

    def wrapper(batch):
        batch_results = []
        for single_id in batch:
            try:
                res = fetch_func(single_id, region, filters)
                if res:
                    batch_results.append(res)
            except Exception as e:
                print(f"Error in {key_name} for ID {single_id}: {e}")
        return batch_results

    batches = [id_list[i:i + batch_size] for i in range(0, len(id_list), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(wrapper, batch) for batch in batches]
        for future in as_completed(futures):
            results.extend(future.result())

    result_map[key_name] = results
               
                all_data = run_multithread_batches(
                    fetch_func=combined_salesorder_fetch,
                    ids=SalesOrderIDs,
                    region=region,
                    filters=filters,
                    batch_size=49,
                    max_workers=30,
                    delay_between_batches=0.5
                )

                result_map['Sales_Order_id'] = all_data
                
                fulfillment_ids = extract_fulfillment_ids_with_map(all_data)
                
                print("Total Fullfillment IDs:", len(fulfillment_ids))

                thread_fetch_and_store(combined_fulfillment_fetch, fulfillment_ids, region, filters, 'Fullfillment Id', result_map)

