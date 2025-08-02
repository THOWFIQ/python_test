import asyncio
import aiohttp
from aiohttp import ClientSession, ClientTimeout
from typing import List

ORDERS_API = "https://salesorderheaderfulfillment-amer.usl-sit-r2-np.kob.dell.com/soheader"
KEYSPHERE_API = "https://keysphereservice-amer.usl-sit-r2-np.kob.dell.com/findby"
BATCH_SIZE = 49
MAX_CONCURRENT_REQUESTS = 20  # controls parallelism for step 9

# GraphQL POST helper
async def graphql_post(session: ClientSession, url: str, query: str):
    headers = {"Content-Type": "application/json"}
    async with session.post(url, json={"query": query}) as resp:
        resp.raise_for_status()
        return await resp.json()

# Step 2: Fetch all orders by date
async def fetch_orders_by_date(session, from_date, to_date):
    query = f'''
    query {{
        getOrdersByDate(fromDate: "{from_date}", toDate: "{to_date}") {{
            result {{
                salesOrderId
                fulfillmentId
            }}
        }}
    }}'''
    response = await graphql_post(session, ORDERS_API, query)
    return response["data"]["getOrdersByDate"]["result"]

# Batched GraphQL request
async def fetch_batch(session, ids: List[str], query_template: str, url: str):
    tasks = []
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i + BATCH_SIZE]
        query = query_template.format(ids=batch)
        tasks.append(graphql_post(session, url, query))
    return await asyncio.gather(*tasks)

# Step 9: Fetch fulfillment detail 1-by-1, throttled with semaphore
async def fetch_fulfillment_detail_throttled(session, fulfillment_id, sem):
    query = f'''
    query {{
        getFulfillmentsBysofulfillmentid(fulfillmentId: "{fulfillment_id}") {{
            salesOrderId
            sourceSystemId
            fulfillments {{
                shipByDate
                oicId
                deliveryCity
                salesOrderLines {{ facility }}
            }}
        }}
    }}'''
    async with sem:
        return await graphql_post(session, ORDERS_API, query)

async def main(from_date, to_date):
    timeout = ClientTimeout(total=120)
    conn = aiohttp.TCPConnector(limit=100)
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with ClientSession(timeout=timeout, connector=conn) as session:
        # Step 1 & 2: Fetch main orders
        order_data = await fetch_orders_by_date(session, from_date, to_date)
        sales_ids = list({item['salesOrderId'] for item in order_data if item.get('salesOrderId')})
        fulfillment_ids = list({item['fulfillmentId'] for item in order_data if item.get('fulfillmentId')})
        print(f"Sales Order IDs: {len(sales_ids)}, Fulfillment IDs: {len(fulfillment_ids)}")

        # Step 7: getBySalesorderids
        sales_query = '''
        query {{
            getBySalesorderids(salesorderIds: {ids}) {{
                result {{
                    salesOrder {{ salesOrderId buid region }}
                    fulfillment {{ fulfillmentId }}
                }}
            }}
        }}'''
        sales_results = await fetch_batch(session, sales_ids, sales_query, KEYSPHERE_API)

        # Step 8: getByFulfillmentids
        fulfillment_query = '''
        query {{
            getByFulfillmentids(fulfillmentIds: {ids}) {{
                result {{
                    salesOrder {{ salesOrderId }}
                    fulfillment {{ fulfillmentId }}
                }}
            }}
        }}'''
        fulfillment_results = await fetch_batch(session, fulfillment_ids, fulfillment_query, KEYSPHERE_API)

        # Step 9: Throttled per-fulfillment-id fetches
        print("Fetching 1-by-1 fulfillment details with throttling...")
        tasks = [fetch_fulfillment_detail_throttled(session, fid, sem) for fid in fulfillment_ids]
        fulfillment_detailed = await asyncio.gather(*tasks)

        # Combine all data
        combined = {
            "orders": order_data,
            "salesResults": sales_results,
            "fulfillmentResults": fulfillment_results,
            "fulfillmentDetails": fulfillment_detailed
        }

        print("✅ Completed all data fetching.")
        return combined

if __name__ == "__main__":
    import time
    from_date = "2024-08-01"
    to_date = "2024-08-15"
    start = time.time()
    result = asyncio.run(main(from_date, to_date))
    print(f"Total Time: {round(time.time() - start, 2)} seconds")
