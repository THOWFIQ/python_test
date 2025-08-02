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

