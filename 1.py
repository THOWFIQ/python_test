import asyncio
import httpx
from httpx import Timeout, RetryError

# --- API URLs ---
GET_ORDERS_URL = "https://salesorderheaderfulfillment-amer.usl-sit-r2-np.kob.dell.com/soheader"
KEYSPHERE_URL = "https://keysphereservice-amer.usl-sit-r2-np.kob.dell.com/findby"

# --- Headers ---
HEADERS = {
    "Content-Type": "application/json"
}

# --- Settings ---
RETRIES = 3
CONCURRENCY_LIMIT = 5
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
TIMEOUT = Timeout(60.0, connect=30.0)

# --- GraphQL Wrapper ---
def build_graphql_query(query: str):
    return {"query": query}

# --- Retryable Post Request ---
async def post_with_retries(url, query_json):
    for attempt in range(RETRIES):
        try:
            async with httpx.AsyncClient(verify=False, timeout=TIMEOUT) as client:
                res = await client.post(url, headers=HEADERS, json=query_json)
                res.raise_for_status()
                return res.json()
        except (httpx.RemoteProtocolError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            print(f"Retry {attempt+1}/{RETRIES} for {url} due to: {e}")
            await asyncio.sleep(1 + attempt)
    raise RetryError(f"Max retries exceeded for {url}")


# --- Step 1-3: Get Orders ---
async def get_orders_by_date(from_date, to_date):
    query = f"""
    query MyQuery {{
        getOrdersByDate(fromDate: "{from_date}", toDate: "{to_date}") {{
            result {{
                fulfillmentId
                oicId
                mustArriveByDate
                salesOrderId
                deliveryCity
                manifestDate
                shipByDate
                shipCode
                updateDate
                soHeaderRef
                systemQty
            }}
        }}
    }}
    """
    json_query = build_graphql_query(query)
    data = await post_with_retries(GET_ORDERS_URL, json_query)
    return data['data']['getOrdersByDate']['result']


# --- Step 6-7: Sales Order Batch Calls ---
async def get_by_salesorder_ids(sales_order_ids):
    batched_results = []

    async def fetch_batch(batch):
        query = f"""
        query MyQuery {{
            getBySalesorderids(salesorderIds: {batch}) {{
                result {{
                    asnNumbers {{ shipDate shipFrom shipTo snNumber sourceManifestId sourceManifestStatus }}
                    fulfillment {{ fulfillmentId fulfillmentStatus oicId sourceSystemStatus }}
                    fulfillmentOrders {{ foId }}
                    salesOrder {{ buid region salesOrderId createDate }}
                    workOrders {{ channelStatusCode woId woStatusCode woType }}
                }}
            }}
        }}
        """
        json_query = build_graphql_query(query)
        return await post_with_retries(KEYSPHERE_URL, json_query)

    batches = [sales_order_ids[i:i+49] for i in range(0, len(sales_order_ids), 49)]
    tasks = [semaphore_wrapper(fetch_batch, str(batch).replace("'", '"')) for batch in batches]
    results = await asyncio.gather(*tasks)

    for r in results:
        batched_results.extend(r['data']['getBySalesorderids']['result'])
    return batched_results


# --- Step 8: Fulfillment ID Batch Calls ---
async def get_by_fulfillment_ids(fulfillment_ids):
    batched_results = []

    async def fetch_batch(batch):
        query = f"""
        query MyQuery {{
            getByFulfillmentids(fulfillmentIds: {batch}) {{
                result {{
                    fulfillment {{ fulfillmentId oicId fulfillmentStatus sourceSystemStatus }}
                    fulfillmentOrders {{ foId }}
                    workOrders {{ woId channelStatusCode woStatusCode woType }}
                    salesOrder {{ salesOrderId buid region createDate }}
                    asnNumbers {{ shipFrom shipTo snNumber sourceManifestId sourceManifestStatus shipDate }}
                }}
            }}
        }}
        """
        json_query = build_graphql_query(query)
        return await post_with_retries(KEYSPHERE_URL, json_query)

    batches = [fulfillment_ids[i:i+49] for i in range(0, len(fulfillment_ids), 49)]
    tasks = [semaphore_wrapper(fetch_batch, str(batch).replace("'", '"')) for batch in batches]
    results = await asyncio.gather(*tasks)

    for r in results:
        batched_results.extend(r['data']['getByFulfillmentids']['result'])
    return batched_results


# --- Step 9: One-by-One Fulfillment Details ---
async def get_fulfillments_by_id(fulfillment_id):
    query = f"""
    query MyQuery {{
        getFulfillmentsBysofulfillmentid(fulfillmentId: "{fulfillment_id}") {{
            fulfillments {{
                shipByDate
                address {{
                    taxRegstrnNum addressLine1 postalCode stateCode cityCode customerNum customerNameExt country createDate
                }}
                oicId shipCode mustArriveByDate updateDate mergeType manifestDate revisedDeliveryDate deliveryCity
                salesOrderLines {{ facility }}
            }}
            sourceSystemId
            salesOrderId
        }}
    }}
    """
    json_query = build_graphql_query(query)
    return await post_with_retries(GET_ORDERS_URL, json_query)


async def get_all_fulfillment_details(fulfillment_ids):
    tasks = [semaphore_wrapper(get_fulfillments_by_id, fid) for fid in fulfillment_ids]
    return await asyncio.gather(*tasks)


# --- Helper: Semaphore wrapper ---
async def semaphore_wrapper(func, *args, **kwargs):
    async with semaphore:
        return await func(*args, **kwargs)


# --- Step 10: Main Pipeline ---
async def main(from_date, to_date):
    print(f"📦 Fetching orders from {from_date} to {to_date}...")
    orders = await get_orders_by_date(from_date, to_date)

    sales_order_ids = list({item['salesOrderId'] for item in orders if item.get('salesOrderId')})
    fulfillment_ids = list({item['fulfillmentId'] for item in orders if item.get('fulfillmentId')})

    print("✅ Sales Order ID Count:", len(sales_order_ids))
    print("✅ Fulfillment ID Count:", len(fulfillment_ids))

    print("🔁 Fetching SalesOrder batch data...")
    sales_order_data = await get_by_salesorder_ids(sales_order_ids)

    print("🔁 Fetching Fulfillment batch data...")
    fulfillment_data = await get_by_fulfillment_ids(fulfillment_ids)

    print("🔁 Fetching individual Fulfillment details...")
    fulfillment_detail_data = await get_all_fulfillment_details(fulfillment_ids)

    combined_data = {
        "orders": orders,
        "sales_order_data": sales_order_data,
        "fulfillment_data": fulfillment_data,
        "fulfillment_detail_data": fulfillment_detail_data
    }

    print("🎉 Data fetch complete.")
    return combined_data


# --- Entry Point ---
if __name__ == "__main__":
    from_date = "2024-06-01"
    to_date = "2024-06-30"
    result = asyncio.run(main(from_date, to_date))
