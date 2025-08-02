import asyncio
import aiohttp
import math
from datetime import datetime

# Configurations
ORDERS_API = "https://salesorderheaderfulfillment-amer.usl-sit-r2-np.kob.dell.com/soheader"
KEYSPHERE_API = "https://keysphereservice-amer.usl-sit-r2-np.kob.dell.com/findby"
BATCH_SIZE = 49

# GraphQL Helper
async def graphql_post(session, url, query):
    headers = {"Content-Type": "application/json"}
    payload = {"query": query}
    async with session.post(url, json=payload, headers=headers, timeout=60) as resp:
        resp.raise_for_status()
        return await resp.json()

# Step 2: Fetch Orders by Date
async def fetch_orders_by_date(session, from_date, to_date):
    query = f'''
    query MyQuery {{
        getOrdersByDate(fromDate: "{from_date}", toDate: "{to_date}") {{
            result {{
                fulfillmentId
                salesOrderId
            }}
        }}
    }}'''
    data = await graphql_post(session, ORDERS_API, query)
    results = data["data"]["getOrdersByDate"]["result"]
    return results

# Step 7: Batched Sales Order ID calls
async def fetch_by_sales_order_ids(session, sales_ids):
    results = []
    for i in range(0, len(sales_ids), BATCH_SIZE):
        batch = sales_ids[i:i+BATCH_SIZE]
        query = f'''
        query MyQuery {{
            getBySalesorderids(salesorderIds: {batch}) {{
                result {{
                    salesOrder {{ salesOrderId buid region createDate }}
                    fulfillment {{ fulfillmentId fulfillmentStatus }}
                    workOrders {{ woId woType woStatusCode }}
                    asnNumbers {{ snNumber shipFrom shipTo }}
                }}
            }}
        }}'''
        results.append(graphql_post(session, KEYSPHERE_API, query))
    return await asyncio.gather(*results)

# Step 8: Batched Fulfillment ID calls
async def fetch_by_fulfillment_ids(session, fulfillment_ids):
    results = []
    for i in range(0, len(fulfillment_ids), BATCH_SIZE):
        batch = fulfillment_ids[i:i+BATCH_SIZE]
        query = f'''
        query MyQuery {{
            getByFulfillmentids(fulfillmentIds: {batch}) {{
                result {{
                    salesOrder {{ salesOrderId buid region }}
                    fulfillment {{ fulfillmentId oicId fulfillmentStatus }}
                    asnNumbers {{ snNumber shipDate }}
                }}
            }}
        }}'''
        results.append(graphql_post(session, KEYSPHERE_API, query))
    return await asyncio.gather(*results)

# Step 9: One-by-one Fulfillment Detail call
async def fetch_fulfillment_detail(session, fulfillment_id):
    query = f'''
    query MyQuery {{
        getFulfillmentsBysofulfillmentid(fulfillmentId: "{fulfillment_id}") {{
            salesOrderId
            sourceSystemId
            fulfillments {{
                oicId shipCode deliveryCity manifestDate
                salesOrderLines {{ facility }}
                address {{ country customerNum customerNameExt cityCode }}
            }}
        }}
    }}'''
    return await graphql_post(session, ORDERS_API, query)

# Combine Step
async def main(from_date, to_date):
    async with aiohttp.ClientSession() as session:
        print(f"Fetching orders from {from_date} to {to_date}...")
        orders = await fetch_orders_by_date(session, from_date, to_date)
        print(f"Total Orders Retrieved: {len(orders)}")

        # Step 3: Extract IDs
        sales_ids = list({o['salesOrderId'] for o in orders if o.get('salesOrderId')})
        fulfillment_ids = list({o['fulfillmentId'] for o in orders if o.get('fulfillmentId')})
        print(f"Sales Order Count: {len(sales_ids)}")
        print(f"Fulfillment ID Count: {len(fulfillment_ids)}")

        # Step 7 & 8: Parallel batch calls
        print("Fetching details by sales order ids...")
        sales_details_responses = await fetch_by_sales_order_ids(session, sales_ids)

        print("Fetching details by fulfillment ids...")
        fulfillment_details_responses = await fetch_by_fulfillment_ids(session, fulfillment_ids)

        # Step 9: Fetch detail for each fulfillmentId (1 by 1)
        print("Fetching final fulfillment details (1-by-1)...")
        detailed_fulfillments = await asyncio.gather(
            *[fetch_fulfillment_detail(session, fid) for fid in fulfillment_ids]
        )

        # Combine all data
        final_result = {
            "orders": orders,
            "sales_details": sales_details_responses,
            "fulfillment_details": fulfillment_details_responses,
            "fulfillment_deep": detailed_fulfillments
        }

        print("✅ Final combined data is ready.")
        return final_result

# Run the function
if __name__ == "__main__":
    from_date = "2024-08-01"
    to_date = "2024-08-15"
    combined_data = asyncio.run(main(from_date, to_date))
    # Do what you want with combined_data, like storing or converting to JSON
