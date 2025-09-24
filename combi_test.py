for item in result_map:
    sales_orders = item.get("data", {}).get("getSalesOrderBySoids", {}).get("salesOrders", [])
    if sales_orders:
        print("✅ Sales Orders found")
    
    work_orders = item.get("data", {}).get("getWorkOrderByWoIds", [])
    if work_orders:
        print("✅ Work Orders found")
