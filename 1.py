import asyncio
import httpx
import math

# Set URLs
GET_ORDERS_URL = "https://salesorderheaderfulfillment-amer.usl-sit-r2-np.kob.dell.com/soheader"
KEYSPHERE_URL = "https://keysphereservice-amer.usl-sit-r2-np.kob.dell.com/findby"

# Headers (if needed)
HEADERS = {
    "Content-Type": "application/json"
}

# GraphQL Helper
def build_graphql_query(query: str):
    return {"query": query}


# Step 1-3: Get Orders by Date
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
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(GET_ORDERS_URL, headers=HEADERS, json=build_graphql_query(query))
        response.raise_for_status()
        data = response.json()['data']['getOrdersByDate']['result']
        return data


# Step 6-7: Batch Sales Order IDs for Keysphere API
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
        async with httpx.AsyncClient(timeout=60.0) as client:
            res = await client.post(KEYSPHERE_URL, headers=HEADERS, json=build_graphql_query(query))
            return res.json()['data']['getBySalesorderids']['result']

    # Batching 49 at a time
    batches = [sales_order_ids[i:i+49] for i in range(0, len(sales_order_ids), 49)]
    tasks = [fetch_batch(str(batch).replace("'", '"')) for batch in batches]
    results = await asyncio.gather(*tasks)
    for r in results:
        batched_results.extend(r)
    return batched_results


# Step 8: Batch Fulfillment IDs
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
        async with httpx.AsyncClient(timeout=60.0) as client:
            res = await client.post(KEYSPHERE_URL, headers=HEADERS, json=build_graphql_query(query))
            return res.json()['data']['getByFulfillmentids']['result']

    # Batching 49 at a time
    batches = [fulfillment_ids[i:i+49] for i in range(0, len(fulfillment_ids), 49)]
    tasks = [fetch_batch(str(batch).replace("'", '"')) for batch in batches]
    results = await asyncio.gather(*tasks)
    for r in results:
        batched_results.extend(r)
    return batched_results


# Step 9: Call getFulfillmentsBysofulfillmentid (one by one)
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
    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(GET_ORDERS_URL, headers=HEADERS, json=build_graphql_query(query))
        return res.json()['data']['getFulfillmentsBysofulfillmentid']


async def get_all_fulfillment_details(fulfillment_ids):
    tasks = [get_fulfillments_by_id(fid) for fid in fulfillment_ids]
    return await asyncio.gather(*tasks)


# Step 10: Main orchestration
async def main(from_date, to_date):
    print(f"Fetching orders from {from_date} to {to_date}...")

    # Step 2-4
    orders = await get_orders_by_date(from_date, to_date)
    sales_order_ids = list({item['salesOrderId'] for item in orders if item.get('salesOrderId')})
    fulfillment_ids = list({item['fulfillmentId'] for item in orders if item.get('fulfillmentId')})

    print("Sales Order ID Count:", len(sales_order_ids))
    print("Fulfillment ID Count:", len(fulfillment_ids))

    # Step 6-8: Parallel API calls
    print("Fetching Keysphere details for Sales Orders...")
    sales_order_data = await get_by_salesorder_ids(sales_order_ids)

    print("Fetching Keysphere details for Fulfillments...")
    fulfillment_data = await get_by_fulfillment_ids(fulfillment_ids)

    print("Fetching detailed Fulfillment information...")
    fulfillment_detail_data = await get_all_fulfillment_details(fulfillment_ids)

    # Combine all data
    combined_data = {
        "orders": orders,
        "sales_order_data": sales_order_data,
        "fulfillment_data": fulfillment_data,
        "fulfillment_detail_data": fulfillment_detail_data
    }

    print("Combined data ready.")
    return combined_data


# Run the full pipeline
if __name__ == "__main__":
    from_date = "2024-06-01"
    to_date = "2024-06-30"
    final_data = asyncio.run(main(from_date, to_date))
