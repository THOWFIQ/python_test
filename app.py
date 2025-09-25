# Extract sales orders (without fulfillments)
sales_orders = [
    so
    for item in result_map
    if isinstance(item.get("data"), dict)
    and "getSalesOrderBySoids" in item["data"]
    and isinstance(item["data"]["getSalesOrderBySoids"].get("salesOrders"), list)
    for so in item["data"]["getSalesOrderBySoids"]["salesOrders"]
]

# Extract fulfillments (from sales orders)
fulfillments = [
    fulfillment
    for item in result_map
    if isinstance(item.get("data"), dict)
    and "getSalesOrderBySoids" in item["data"]
    and isinstance(item["data"]["getSalesOrderBySoids"].get("salesOrders"), list)
    for so in item["data"]["getSalesOrderBySoids"]["salesOrders"]
    for fulfillment in so.get("fulfillments", [])
]

# Extract work orders
work_orders = [
    wo
    for item in result_map
    if isinstance(item.get("data"), dict)
    and "getWorkOrderByWoIds" in item["data"]
    and isinstance(item["data"]["getWorkOrderByWoIds"], list)
    for wo in item["data"]["getWorkOrderByWoIds"]
]
