if "Sales_Order_id" in filters:
        REGION = ""
        salesOrder_key = "Sales_Order_id"
        matched = False
        uniqueSalesOrder_ids = ",".join(sorted(set(filters[salesOrder_key].split(','))))
        filters[salesOrder_key] = uniqueSalesOrder_ids
       
        if filters.get(salesOrder_key):
            salesorder_ids = list(map(str.strip, filters[salesOrder_key].split(",")))
            for soid in chunk_list(salesorder_ids,10):
                payload = {"query": fetch_keysphereSalesorder_query(soid)}
                response = requests.post(path['FID'], json=payload, verify=False)
                data = response.json()

                if "errors" in data:
                    continue
               
                result = data.get("data", {}).get("getBySalesorderids", {})
                # print(json.dumps(result,indent=2))
                # exit()
                new_items = list(map(
                    lambda item: {
                        "salesOrderId": item.get("salesOrder", {}).get("salesOrderId"),
                        "region": item.get("salesOrder", {}).get("region"),
                        "workOrderIds": list(map(
                            lambda wo: wo.get("woId"),
                            filter(lambda wo: wo.get("woId"), item.get("workOrders", []))
                        ))
                    },
                    result.get("result", [])
                ))

                finalResult.extend(new_items)

                work_order_ids = ",".join(sorted(set(map(
                                                    lambda wo: wo.get("woId"),
                                                    [wo for item in result.get("result", []) for wo in item.get("workOrders", []) if wo.get("woId")]
                                                ))))

                wo_ids = list(map(str.strip, work_order_ids.split(",")))
                # print(json.dumps(wo_ids))
                # exit()
                graphql_request.append({
                        "url": path['SALESFULLFILLMENT'],
                        "query": fetch_salesOrder_query(soid)
                    })
                
                graphql_request.append({
                        "url": path['WORKORDER'],
                        "query": fetch_workOrder_query(wo_ids)
                    })

    results = asyncio.run(run_all(graphql_request))
    return results

async def fetch_graphql(session, url, query):
    async with session.post(url, json={"query": query}) as response:
        return await response.json()

async def run_all(graphql_request):
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_graphql(session, req["url"], req["query"])
            for req in graphql_request
        ]
        results = await asyncio.gather(*tasks)
        return results
