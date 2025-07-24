import asyncio
import json

# --- Async helpers ---

async def safe_fetch(fetch_func, id_batch, region, filters):
    try:
        return await fetch_func(id_batch, region, filters)
    except Exception as e:
        print(f"[ERROR] Fetch failed for batch {id_batch}: {e}")
        return {}

async def run_async_batches(fetch_func, id_list, region, filters, batch_size=10, concurrency_limit=5):
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def bound_fetch(id_batch):
        async with semaphore:
            return await safe_fetch(fetch_func, id_batch, region, filters)

    tasks = []
    for i in range(0, len(id_list), batch_size):
        batch = id_list[i:i + batch_size]
        tasks.append(bound_fetch(batch))

    results = await asyncio.gather(*tasks)
    return [res for res in results if res]  # Remove failed/empty ones

# --- Async wrapper for fetch functions that return only one result (like fulfillment/woid/foid) ---
async def fetch_and_store(fetch_func, id_list, region, filters, key, result_map):
    results = await run_async_batches(fetch_func, id_list, region, filters)
    result_map[key] = results

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

    # Async fetch for sales orders
    all_data = asyncio.run(run_async_batches(
        combined_salesorder_fetch,
        SalesOrderIDs,
        region,
        filters,
        batch_size=10,
        concurrency_limit=5
    ))

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

    # Async fetch for fulfillment/foid/woid in parallel
    async def fetch_all_secondary():
        await asyncio.gather(
            fetch_and_store(combined_fulfillment_fetch, fulfillment_ids, region, filters, 'Fullfillment Id', result_map),
            fetch_and_store(combined_foid_fetch, fo_ids, region, filters, 'foid', result_map),
            fetch_and_store(combined_woid_fetch, wo_ids, region, filters, 'wo_id', result_map),
        )

    asyncio.run(fetch_all_secondary())

else:
    print("Not coming order date part")

print(json.dumps(result_map, indent=2))
exit()
