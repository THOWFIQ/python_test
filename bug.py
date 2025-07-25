from concurrent.futures import ThreadPoolExecutor, as_completed

def run_multithread_batches(fetch_func, ids, region, filters, batch_size=10, max_workers=5):
    all_results = []

    def wrapper(batch_ids):
        batch_results = []
        for i in batch_ids:
            try:
                res = fetch_func(i, region, filters)
                batch_results.append(res)
            except Exception as e:
                print(f"Error fetching ID {i}: {e}")
        return batch_results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(wrapper, ids[i:i + batch_size]) for i in range(0, len(ids), batch_size)]
        for future in as_completed(futures):
            try:
                all_results.extend(future.result())
            except Exception as e:
                print(f"Batch error: {e}")
    
    return all_results

def thread_fetch_and_store(fetch_func, id_list, region, filters, key_name, result_map):
    results = []

    def wrapper(single_id):
        try:
            return fetch_func(single_id, region, filters)
        except Exception as e:
            print(f"Error in {key_name} for ID {single_id}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(wrapper, i) for i in id_list]
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    result_map[key_name] = results

from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# --- Main logic ---
if (
    'Order_from_date' in primary_filters and
    'Order_to_date' in primary_filters and
    not any(k in primary_filters for k in ['Sales_Order_id', 'Fullfillment Id', 'foid', 'wo_id'])
):
    print("I'm Order Date Part")

    from_date = primary_filters.get('Order_from_date')
    to_date = primary_filters.get('Order_to_date')

    OrderDate_Response = combined_OrderDate_fetch(from_date, to_date, region, filters)
    resultData = OrderDate_Response.get('data', {}).get('getOrdersByDate', {}).get('result', [])
    SalesOrderIDs = [item.get('salesOrderId') for item in resultData if item.get('salesOrderId')]

    print("Total Sales Order IDs:", len(SalesOrderIDs))

    # Multi-threaded fetch for sales orders
    all_data = run_multithread_batches(combined_salesorder_fetch, SalesOrderIDs, region, filters)
    result_map['Sales_Order_id'] = all_data

    # Extract unique IDs
    fulfillment_ids = []
    fo_ids = []
    wo_ids = []

    for salesData in all_data:
        try:
            result = salesData.get('data', {}).get('getBySalesorderids', {}).get('result', [])
            if not result:
                continue

            first_result = result[0]
            fulfillment = first_result.get('fulfillment', [])
            fulfillment_id = fulfillment[0].get('fulfillmentId') if fulfillment else None
            if fulfillment_id and ('Fullfillment Id' not in filters or fulfillment_id == filters.get('Fullfillment Id')):
                fulfillment_ids.append(fulfillment_id)

            fulfillment_orders = first_result.get('fulfillmentOrders', [])
            foid = fulfillment_orders[0].get('foId') if fulfillment_orders else None
            if foid and ('foid' not in filters or foid == filters.get('foid')):
                fo_ids.append(foid)

            work_orders = first_result.get('workOrders', [])
            woid = work_orders[0].get('woId') if work_orders else None
            if woid and ('wo_id' not in filters or woid == filters.get('wo_id')):
                wo_ids.append(woid)

        except Exception as e:
            print(f"Error processing salesData: {e}")

    # Multi-thread fetch for fulfillment/foid/woid
    thread_fetch_and_store(combined_fulfillment_fetch, fulfillment_ids, region, filters, 'Fullfillment Id', result_map)
    thread_fetch_and_store(combined_foid_fetch, fo_ids, region, filters, 'foid', result_map)
    thread_fetch_and_store(combined_woid_fetch, wo_ids, region, filters, 'wo_id', result_map)

else:
    print("Not coming order date part")

print(json.dumps(result_map, indent=2))
exit()
