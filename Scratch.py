if (
            'Order_from_date' in primary_filters and
            'Order_to_date' in primary_filters and
            not any(k in primary_filters for k in ['Sales_Order_id', 'Fullfillment Id', 'foid', 'wo_id'])
        ):
            print("I'm Order Date Part")

            from_date = primary_filters.get('Order_from_date')
            to_date = primary_filters.get('Order_to_date')

            # Fetch sales order IDs by date
            OrderDate_Response = combined_OrderDate_fetch(from_date, to_date, region, filters)
            resultData = OrderDate_Response.get('data', {}).get('getOrdersByDate', {}).get('result', [])
            SalesOrderIDs = [item.get('salesOrderId') for item in resultData if item.get('salesOrderId')]

            print("Total Sales Order IDs:", len(SalesOrderIDs))

            # Async fetch all sales order data in batches
            all_data = asyncio.run(run_async_batches(combined_salesorder_fetch, SalesOrderIDs, region, filters))
            result_map['Sales_Order_id'] = all_data

            for salesData in all_data:
                try:
                    result = salesData.get('data', {}).get('getBySalesorderids', {}).get('result', [])
                    if not result:
                        print("No result found.")
                        continue

                    first_result = result[0]
                    sales_order_id = first_result.get('salesOrder', {}).get('salesOrderId')

                    fulfillment = first_result.get('fulfillment', [])
                    fill = fulfillment[0].get('fulfillmentId') if fulfillment else None

                    fulfillment_orders = first_result.get('fulfillmentOrders', [])
                    foid = fulfillment_orders[0].get('foId') if fulfillment_orders else None

                    work_orders = first_result.get('workOrders', [])
                    if not work_orders:
                        print(f"Skipping Sales Order {sales_order_id} due to missing workOrders.")
                        continue

                    woid = work_orders[0].get('woId')
                    print(f"Sales Order: {sales_order_id} | Fulfillment ID: {fill} | FO ID: {foid} | WO ID: {woid}")

                    # Fulfillment
                    if fill and ('Fullfillment Id' not in filters or fill == filters.get('Fullfillment Id')):
                        try:
                            threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                            result_map['Fullfillment Id'] = threadRes
                        except Exception as e:
                            print(f"[ERROR] Fulfillment Thread from Sales: {e}")

                    # FOID
                    if foid and ('foid' not in filters or foid == filters.get('foid')):
                        try:
                            threadRes = threadFunction(combined_foid_fetch, [foid], format_type, region, filters)
                            result_map['foid'] = threadRes
                        except Exception as e:
                            print(f"[ERROR] FOID Thread from Sales: {e}")

                    # WOID
                    if woid and ('wo_id' not in filters or woid == filters.get('wo_id')):
                        try:
                            threadRes = threadFunction(combined_woid_fetch, [woid], format_type, region, filters)
                            result_map['wo_id'] = threadRes
                        except Exception as e:
                            print(f"[ERROR] WOID Thread from Sales: {e}")

                except Exception as e:
                    print(f"Error processing salesData: {e}")

        else:
            print("Not coming order date part")

        print(json.dumps(result_map, indent=2))
        exit()
