from flask import request, jsonify
import requests
import httpx
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

PRIMARY_FIELDS = {
    "Sales_Order_id", "wo_id", "Fullfillment Id", "foid", "order_date","Order_from_date","Order_to_date"
}
SECONDARY_FIELDS = {
    "ISMULTIPACK", "BUID", "Facility"
}

def getPath(region):
    try:
        if region == "EMEA":
            return {
                "FID": configPath['Linkage_EMEA'],
                "FOID": configPath['FM_Order_EMEA_APJ'],
                "SOPATH": configPath['SO_Header_EMEA_APJ'],
                "WOID": configPath['WO_Details_EMEA_APJ'],
                "FFBOM": configPath['FM_BOM_EMEA_APJ']
            }
        elif region == "APJ":
            return {
                "FID": configPath['Linkage_APJ'],
                "FOID": configPath['FM_Order_EMEA_APJ'],
                "SOPATH": configPath['SO_Header_EMEA_APJ'],
                "WOID": configPath['WO_Details_EMEA_APJ'],
                "FFBOM": configPath['FM_BOM_EMEA_APJ']
            }
        elif region in ["DAO","AMER","LA"]:
            return {
                "FID": configPath['Linkage_DAO'],
                "FOID": configPath['FM_Order_DAO'],
                "SOPATH": configPath['SO_Header_DAO'],
                "WOID": configPath['WO_Details_DAO'],
                "FFBOM": configPath['FM_BOM_DAO']
            }
    except Exception as e:
        print(f"[ERROR] getPath failed: {e}")
        traceback.print_exc()
        return {}

def post_api(URL, query, variables):
    try:
        if variables:
            response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False)
        else:
            response = httpx.post(URL, json={"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"[ERROR] post_api failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}

def combined_salesorder_fetch(so_id, region, filters):
    combined_salesorder_data = {'data': {}}
    try:
        path = getPath(region)

        soi = {"salesorderIds": [so_id]}
        if so_id is not None:
            soaorder_query = fetch_soaorder_query()
            soaorder = post_api(URL=path['SOPATH'], query=soaorder_query, variables=soi)

            if soaorder and soaorder.get('data'):
                combined_salesorder_data['data']['getSoheaderBySoids'] = soaorder['data'].get('getSoheaderBySoids', [])

            salesorder_query = fetch_salesorder_query(so_id)
            salesorder = post_api(URL=path['FID'], query=salesorder_query, variables=None)

            if salesorder and salesorder.get('data') and 'getBySalesorderids' in salesorder['data']:
                combined_salesorder_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']

        return combined_salesorder_data
    except Exception as e:
        print(f"Error in combined_salesorder_fetch: {e}")
        return {}

def combined_fulfillment_fetch(fulfillment_id, region, filters):
    combined_fullfillment_data = {'data': {}}
    try:
        path = getPath(region)
        
        combined_fullfillment_data = {'data': {}}
        ffQid = {"fulfillment_id": fulfillment_id}

        # Fetch fulfillment data
        fulfillment_query = fetch_fulfillment_query()
        fulfillment_data = post_api(URL=path['SOPATH'], query=fulfillment_query, variables=ffQid)
        
        if fulfillment_data and fulfillment_data.get('data'):
            fulfillments = fulfillment_data['data'].get('getFulfillmentsById', [])
            combined_fullfillment_data['data']['getFulfillmentsById'] = fulfillments

            if fulfillments and isinstance(fulfillments, list):
                salesOrderID = fulfillments[0].get('salesOrderId')
            else:
                raise ValueError("No valid fulfillment data found.")
        else:
            raise ValueError("No data returned for fulfillment query.")

        # Fetch sofulfillment data
        sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
        sofulfillment_data = post_api(URL=path['SOPATH'], query=sofulfillment_query, variables=ffQid)
        
        if sofulfillment_data and sofulfillment_data.get('data'):
            combined_fullfillment_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})

        # Fetch directship data
        directship_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
        directship_data = post_api(URL=path['FOID'], query=directship_query, variables=ffQid)
        
        if directship_data and directship_data.get('data'):
            combined_fullfillment_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = directship_data['data'].get('getAllFulfillmentHeadersSoidFulfillmentid', {})

        # Fetch FBOM data
        fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
        fbom_data = post_api(URL=path['FFBOM'], query=fbom_query, variables=None)
        
        if fbom_data and fbom_data.get('data'):
            combined_fullfillment_data['data']['getFbomBySoFulfillmentid'] = fbom_data['data'].get('getFbomBySoFulfillmentid', {})

        # Fetch sales order data using salesOrderID
        if salesOrderID:
            ffoid_query = fetch_salesorder_query(salesOrderID)
            ffoidData = post_api(URL=path['FID'], query=ffoid_query, variables=None)
            
            if ffoidData and ffoidData.get('data'):
                combined_fullfillment_data['data']['getBySalesorderids'] = ffoidData['data'].get('getBySalesorderids', [])
        else:
            raise ValueError("Sales Order ID not found in fulfillment data.")

        return combined_fullfillment_data

    except Exception as e:
        print(f"Error in combined_fulfillment_fetch: {e}")
        return {}

def combined_foid_fetch(fo_id, region, filters):
    
    combined_foid_data = {'data': {}}
    try:
        path = getPath(region)

        combined_foid_data = {'data': {}}

        foid_query = fetch_foid_query(fo_id)
        foid_output = post_api(URL=path['FOID'], query=foid_query, variables=None)

        if foid_output and foid_output.get('data'):
            foid_records = foid_output['data'].get('getAllFulfillmentHeadersByFoId', [])
            combined_foid_data['data']['getAllFulfillmentHeadersByFoId'] = foid_records
        else:
            foid_records = []

        if foid_records:
            fulfillment_id = foid_records[0].get('fulfillmentId')
            if fulfillment_id:
                fulfillment_query = fetch_getByFulfillmentids_query(fulfillment_id)
                fulfillment_data = post_api(URL=path['FID'], query=fulfillment_query, variables=None)

                if fulfillment_data and fulfillment_data.get('data'):
                    combined_foid_data['data']['getByFulfillmentids'] = fulfillment_data['data'].get('getByFulfillmentids', [])

        return combined_foid_data

    except Exception as e:
        print(f"Error in combined_foid_fetch: {e}")
        return {}


def combined_woid_fetch(wo_id, region, filters):
    combined_wo_data = {'data': {}}
    try:
        path = getPath(region)

        combined_wo_data = {'data': {}}

        wo_query = fetch_getByWorkorderids_query(wo_id)

        wo_data = post_api(URL=path['FID'], query=wo_query, variables=None)

        if wo_data and wo_data.get('data'):
            combined_wo_data['data']['getByWorkorderids'] = wo_data['data'].get('getByWorkorderids', {})

        return combined_wo_data

    except Exception as e:
        print(f"[ERROR] Error in combined_woid_fetch: {e}")
        return {}

def combined_OrderDate_fetch(orderFromDate, orderToDate, region, filters):
    
    combined_orderDate_data = {'data': {}}
    try:
        path = getPath(region)
        orderDate_query = fetch_getOrderDate_query(orderFromDate, orderToDate)

        orderDate_data = post_api(URL=path['SOPATH'], query=orderDate_query, variables=None)
        
        if orderDate_data and orderDate_data.get('data'):
            combined_orderDate_data['data']['getOrdersByDate'] = orderDate_data['data'].get('getOrdersByDate', {})
        
        return combined_orderDate_data

    except Exception as e:
        print(f"[ERROR] Error in combined_orderDate_data_fetch: {e}")
        return {}

def run_multithread_batches(fetch_func, ids, region, filters, batch_size=10, max_workers=5, delay_between_batches=1):
    all_results = []

    def wrapper(batch_ids):
        batch_results = []
        for i in batch_ids:
            try:
                res = fetch_func(i, region, filters)
                batch_results.append(res)
            except Exception as e:
                print(f"Error fetching ID {i}: {e}")
        return batch_results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            print(f"\nProcessing batch {i//batch_size + 1} ({len(batch)} items)...")

            future = executor.submit(wrapper, batch)
            try:
                batch_results = future.result(timeout=60)  # timeout per batch
                all_results.extend(batch_results)
            except Exception as e:
                print(f"Timeout or error in batch {i//batch_size + 1}: {e}")
            
            time.sleep(delay_between_batches)

    return all_results

def thread_fetch_and_store(fetch_func, id_list, region, filters, key_name, result_map, max_workers=5):
    results = []

    def wrapper(single_id):
        try:
            return fetch_func(single_id, region, filters)
        except Exception as e:
            print(f"Error in {key_name} for ID {single_id}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(wrapper, i) for i in id_list]
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    result_map[key_name] = results


def fieldValidation(filters, format_type, region):
    data_row_export = {}
    primary_in_filters = []
    secondary_in_filters = []

    try:
        for field in filters:
            if field in PRIMARY_FIELDS:
                primary_in_filters.append(field)
            elif field in SECONDARY_FIELDS:
                secondary_in_filters.append(field)

        if not primary_in_filters:
            return {
                "status": "error",
                "message": "At least one primary field is required in filters."
            }

        primary_filters = {key: filters[key] for key in primary_in_filters}
        secondary_filters = {key: filters[key] for key in secondary_in_filters}
        result_map = {}
       
        # ---------- Sales Order Block ----------
        if 'Sales_Order_id' in primary_filters: 
            try:
                so_ids = list(set(x.strip() for x in primary_filters['Sales_Order_id'].split(',') if x.strip()))
                
                threadRes = threadFunction(combined_salesorder_fetch, so_ids, format_type, region, filters)
                
                result_map['Sales_Order_id'] = threadRes
                
                for salesData in threadRes:
                    try:
                        result = salesData.get('data', {}).get('getBySalesorderids', {}).get('result', [])
                        if not result:
                            raise ValueError("Missing or empty 'result' list.")

                        first_result = result[0]

                        fulfillment = first_result.get('fulfillment', [])
                        if not fulfillment:
                            raise ValueError("Missing or empty 'fulfillment' list.")
                        fill = fulfillment[0].get('fulfillmentId')
                        if fill is None:
                            raise ValueError("Missing 'fulfillmentId'.")

                        fulfillment_orders = first_result.get('fulfillmentOrders', [])
                        if not fulfillment_orders:
                            raise ValueError("Missing or empty 'fulfillmentOrders' list.")
                        foid = fulfillment_orders[0].get('foId')
                        if foid is None:
                            raise ValueError("Missing 'foId'.")

                        work_orders = first_result.get('workOrders', [])
                        if not work_orders:
                            raise ValueError("Missing or empty 'workOrders' list.")
                        woid = work_orders[0].get('woId')
                        if woid is None:
                            raise ValueError("Missing 'woId'.")

                        # You can now safely use fill, foid, woid
                        print(f"Fulfillment ID: {fill}, FO ID: {foid}, WO ID: {woid}")

                    except Exception as e:
                        print(f"[ERROR] Parsing salesData: {e}")
                        continue
                    
                    # Fulfillment
                    try:                        
                        if 'Fullfillment Id' in filters and fill in [filters['Fullfillment Id']]:                             
                            threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                            result_map['Fullfillment Id'] = threadRes
                            
                        elif 'Fullfillment Id' not in filters:
                            threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                            result_map['Fullfillment Id'] = threadRes
                            
                    except Exception as e:
                        print(f"[ERROR] Fulfillment Thread from Sales: {e}")

                    # FOID
                    try:
                        if 'foid' in filters and foid in [filters['foid']]:
                            threadRes = threadFunction(combined_foid_fetch, [foid], format_type, region, filters)
                            result_map['foid'] = threadRes
                        else:
                            threadRes = threadFunction(combined_foid_fetch, [foid], format_type, region, filters)
                            result_map['foid'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] FOID Thread from Sales: {e}")

                    # WOID
                    try:
                        if 'wo_id' in filters and woid in [filters['wo_id']]:
                            threadRes = threadFunction(combined_woid_fetch, [woid], format_type, region, filters)
                            result_map['wo_id'] = threadRes
                        else:
                            threadRes = threadFunction(combined_woid_fetch, [woid], format_type, region, filters)
                            result_map['wo_id'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] WOID Thread from Sales: {e}")
            except Exception as e:
                print(f"[ERROR] Sales_Order_id thread block: {e}")

        # ---------- Fulfillment Block ----------
        if 'Fullfillment Id' in primary_filters and not any(k in primary_filters for k in ['Sales_Order_id']):
            print('sales not in')
            try:
                fil_ids = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))
                threadRes = threadFunction(combined_fulfillment_fetch, fil_ids, format_type, region, filters)
                result_map['Fullfillment Id'] = threadRes
                
                for fullfilData in threadRes:
                    try:
                        sales = fullfilData.get('data', {}).get('getFulfillmentsById', [])
                        if not sales:
                            raise ValueError("Missing or empty 'getFulfillmentsById' list.")
                        sales_id = sales[0].get('salesOrderId')
                        if sales_id is None:
                            raise ValueError("Missing 'salesOrderId'.")

                        result = fullfilData.get('data', {}).get('getBySalesorderids', {}).get('result', [])
                        if not result:
                            raise ValueError("Missing or empty 'result' list.")

                        fulfillment_orders = result[0].get('fulfillmentOrders', [])
                        if not fulfillment_orders:
                            raise ValueError("Missing or empty 'fulfillmentOrders' list.")
                        foid = fulfillment_orders[0].get('foId')
                        if foid is None:
                            raise ValueError("Missing 'foId'.")

                        work_orders = result[0].get('workOrders', [])
                        if not work_orders:
                            raise ValueError("Missing or empty 'workOrders' list.")
                        woid = work_orders[0].get('woId')
                        if woid is None:
                            raise ValueError("Missing 'woId'.")

                        # You can now safely use sales_id, foid, woid
                        print(f"Sales Order ID: {sales_id}, FO ID: {foid}, WO ID: {woid}")

                    except Exception as e:
                        print(f"[ERROR] Parsing fullfilData: {e}")

                    try:
                        if 'Sales_Order_id' not in filters or sales_id in [filters['Sales_Order_id']]:
                            threadRes = threadFunction(combined_salesorder_fetch, [sales_id], format_type, region, filters)
                            result_map['Sales_Order_id'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] Sales from Fulfillment: {e}")

                    try:
                        threadRes = threadFunction(combined_foid_fetch, [foid], format_type, region, filters)
                        result_map['foid'] = threadRes

                    except Exception as e:
                        print(f"[ERROR] FOID from Fulfillment: {e}")

                    try:
                        threadRes = threadFunction(combined_woid_fetch, [woid], format_type, region, filters)
                        result_map['wo_id'] = threadRes
                    except Exception as e:
                        print(f"[ERROR] WOID from Fulfillment: {e}")
            except Exception as e:
                print(f"[ERROR] Fulfillment thread block: {e}")

        # ---------- FOID Block ----------
        if 'foid' in primary_filters and not any(k in primary_filters for k in ['Sales_Order_id', 'Fullfillment Id']):
            print('sales and fill not in')
            try:
                fo_ids = list(set(x.strip() for x in primary_filters['foid'].split(',') if x.strip()))
                threadRes = threadFunction(combined_foid_fetch, fo_ids, format_type, region, filters)
                result_map['foid'] = threadRes
                
                
                for FOData in threadRes:
                    try:
                        result = FOData.get('data', {}).get('getAllFulfillmentHeadersByFoId', {})
                        result_ids = FOData.get('data', {}).get('getByFulfillmentids', {}).get('result', [])
                        
                        if not result and result_ids:
                            raise ValueError("Missing or empty 'result' list.")

                        first_result = result[0]
                        second_result = result_ids[0]

                        sales = first_result.get('salesOrderId')
                        if sales is None:
                            raise ValueError("Missing 'salesOrderId'.")
                        
                        fill = first_result.get('fulfillmentId')
                        if fill is None:
                            raise ValueError("Missing 'fulfillmentId'.")

                        work_orders = second_result.get('workOrders', [])
                        if not work_orders:
                            raise ValueError("Missing or empty 'workOrders' list.")
                        woid = work_orders[0].get('woId')
                        if woid is None:
                            raise ValueError("Missing 'woId'.")
                        
                        # You can now safely use sales, fill, woid
                        print(f"Sales Order ID: {sales}, Fulfillment ID: {fill}, WO ID: {woid}")

                    except Exception as e:
                        print(f"[ERROR] Parsing FOData: {e}")

                    try:
                        threadRes = threadFunction(combined_salesorder_fetch, [sales], format_type, region, filters)
                        result_map['Sales_Order_id'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] Sales from FOID: {e}")

                    try:
                        threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                        result_map['Fullfillment Id'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] Fulfillment from FOID: {e}")

                    try:
                        threadRes = threadFunction(combined_woid_fetch, [woid], format_type, region, filters)
                        result_map['wo_id'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] WOID from FOID: {e}")
            except Exception as e:
                print(f"[ERROR] FOID thread block: {e}")

        # # ---------- WOID Block ----------
        if 'wo_id' in primary_filters and not any(k in primary_filters for k in ['Sales_Order_id', 'Fullfillment Id', 'foid']):
            print('sales and fill , fo not in')
            try:
                wo_ids = list(set(x.strip() for x in primary_filters['wo_id'].split(',') if x.strip()))
                threadRes = threadFunction(combined_woid_fetch, wo_ids, format_type, region, filters)
                result_map['WOID'] = threadRes
                
                for WOData in threadRes:
                    try:
                        result = WOData.get('data', {}).get('getByWorkorderids', {}).get('result', [])
                        if not result:
                            raise ValueError("Missing or empty 'result' list.")

                        first_result = result[0]

                        fulfillment = first_result.get('fulfillment', {})
                        if not fulfillment:
                            raise ValueError("Missing or empty 'fulfillment' list.")
                        fill = fulfillment.get('fulfillmentId')
                        if fill is None:
                            raise ValueError("Missing 'fulfillmentId'.")

                        fulfillment_orders = first_result.get('fulfillmentOrders', [])
                        if not fulfillment_orders:
                            raise ValueError("Missing or empty 'fulfillmentOrders' list.")
                        foid = fulfillment_orders[0].get('foId')
                        if foid is None:
                            raise ValueError("Missing 'foId'.")

                        sales_orders = first_result.get('salesOrder', {})
                        
                        if not sales_orders:
                            raise ValueError("Missing or empty 'workOrders' list.")
                        soisid = sales_orders.get('salesOrderId')
                        if soisid is None:
                            raise ValueError("Missing 'woId'.")

                        print(f"Fulfillment ID: {fill}, FO ID: {foid}, soi ID: {soisid}")

                    except Exception as e:
                        print(f"[ERROR] Parsing salesData: {e}")


                    try:
                        threadRes = threadFunction(combined_salesorder_fetch, [soisid], format_type, region, filters)
                        result_map['Sales_Order_id'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] Sales from WOID: {e}")

                    try:
                        threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                        result_map['Fullfillment Id'] = threadRes
                        
                    except Exception as e:
                        print(f"[ERROR] Fulfillment from WOID: {e}")

                    try:
                        threadRes = threadFunction(combined_foid_fetch, [foid], format_type, region, filters)
                        result_map['foid'] = threadRes
                       
                    except Exception as e:
                        print(f"[ERROR] FOID from WOID: {e}")
            except Exception as e:
                print(f"[ERROR] WOID thread block: {e}")

        # # ---------- OrderDate  Block ----------
        if 'Order_from_date' in primary_filters and 'Order_to_date' in primary_filters and not any(k in primary_filters for k in ['Sales_Order_id', 'Fullfillment Id', 'foid', 'wo_id']):
            print("I'm in Order Date Part")
            
            start_time = time.time()
            
            from_date = primary_filters.get('Order_from_date')
            to_date = primary_filters.get('Order_to_date')

            # Step 1: Fetch order IDs
            OrderDate_Response = combined_OrderDate_fetch(from_date, to_date, region, filters)
            resultData = OrderDate_Response.get('data', {}).get('getOrdersByDate', {}).get('result', [])
            # start_time = time.time()
            SalesOrderIDs = [item.get("salesOrderId") for item in resultData if item.get("salesOrderId")]
            print("Total Sales Order IDs:", len(SalesOrderIDs))
                       
            # Step 2: Multi-threaded fetch for each sales order
            all_data = run_multithread_batches(
                fetch_func=combined_salesorder_fetch,
                ids=SalesOrderIDs,
                region=region,
                filters=filters,
                batch_size=5,
                max_workers=5,
                delay_between_batches=1
            )
            end_time = time.time()
           
            result_map['Sales_Order_id'] = all_data
                       
            # Step 3: Extract fulfillmentId, foId, woId
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

            # Step 4: Threaded fetch for fulfillment, foid, woid
            thread_fetch_and_store(combined_fulfillment_fetch, fulfillment_ids, region, filters, 'Fullfillment Id', result_map)
            thread_fetch_and_store(combined_foid_fetch, fo_ids, region, filters, 'foid', result_map)
            thread_fetch_and_store(combined_woid_fetch, wo_ids, region, filters, 'wo_id', result_map)

        else:
            print("Not entering Order Date part")
        
        return result_map

    except Exception as e:
        print(f"[FATAL ERROR] in fieldValidation: {e}")
        return {"status": "error", "message": "Unexpected failure during validation"}

# def threadFunction(functionName, ids,  format_type, region, filters):
    
#     result = []
#     with ThreadPoolExecutor(max_workers=5) as executor:
#         futures = [executor.submit(functionName, id, region, filters) for id in ids]
#         for future in as_completed(futures):
#             try:
#                 res = future.result()
#                 if res:
#                     result.append(res)
#             except Exception as e:
#                 print(f"[ERROR] Thread Function fetch failed: {e}")
#     return result

# from concurrent.futures import ThreadPoolExecutor, as_completed

def threadFunction(fetch_function, id_list, format_type, region, filters):
    results = []

    def wrapper(so_id):
        try:
            return fetch_function(so_id, region, filters)
        except Exception as e:
            print(f"Error fetching data for SO ID {so_id}: {e}")
            return {}

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(wrapper, so_id): so_id for so_id in id_list}
        for future in as_completed(future_to_id):
            so_id = future_to_id[future]
            try:
                data = future.result()
                results.append({so_id: data})
            except Exception as e:
                print(f"Thread failed for SO ID {so_id}: {e}")
                results.append({so_id: {}})

    return results


def OutputFormat(result_map, format_type=None,region=None):
    try:
        flat_list = []
        sales_orders = result_map.get("Sales_Order_id", [])
        fulfillments = result_map.get("Fullfillment Id", [])
        wo_ids = result_map.get("wo_id", [])
        foid_data = result_map.get("foid", [])

        for so_index, so_entry in enumerate(sales_orders):
            try:
                if not isinstance(so_entry, dict):
                    continue
                so_data = so_entry.get("data", {})
                get_soheaders = so_data.get("getSoheaderBySoids", [])
                get_salesorders = so_data.get("getBySalesorderids", {})

                if not get_soheaders or not get_salesorders:
                    continue

                soheader = get_soheaders[0] if isinstance(get_soheaders, list) else {}
                salesorder = get_salesorders.get("result", [{}])[0]

                fulfillment, sofulfillment = {}, {}
                if so_index < len(fulfillments):
                    fulfillment_entry = fulfillments[so_index]
                    if isinstance(fulfillment_entry, dict):
                        fulfillment_data = fulfillment_entry.get("data", {})
                        f_raw = fulfillment_data.get("getFulfillmentsById", [])
                        s_raw = fulfillment_data.get("getFulfillmentsBysofulfillmentid", [])
                        
                        fulfillment = f_raw[0] if f_raw else {}
                        sofulfillment = s_raw[0] if s_raw else {}

                base_row = {
                    "BUID": soheader.get("buid"),
                    "PP Date": soheader.get("ppDate"),
                    "Sales Order Id": soheader.get("salesOrderId"),
                    "Fulfillment Id": salesorder['fulfillment'][0]['fulfillmentId'] if salesorder.get('fulfillment') else "",
                    "Region Code": salesorder.get('salesOrder', {}).get('region', ""),
                    "FoId": salesorder['fulfillmentOrders'][0]['foId'] if salesorder.get('fulfillmentOrders') else "",
                    "System Qty": fulfillment.get('fulfillments', [{}])[0].get('systemQty', "") if fulfillment.get('fulfillments') is not None else "",
                    "Ship By Date": fulfillment.get('fulfillments', [{}])[0].get('shipByDate', ""),
                    "LOB": fulfillment.get('fulfillments', [{}])[0].get('salesOrderLines', [{}])[0].get('lob', ""),
                    "Ship From Facility": "",
                    "Ship To Facility": "",
                    "Tax Regstrn Num": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('taxRegstrnNum', ""),
                    "Address Line1": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('addressLine1', ""),
                    "Postal Code": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('postalCode', ""),
                    "State Code": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('stateCode', ""),
                    "City Code": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('cityCode', ""),
                    "Customer Num": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('customerNum', ""),
                    "Customer Name Ext": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('customerNameExt', ""),
                    "Country": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('country', ""),
                    "Create Date": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('createDate', ""),
                    "Ship Code": sofulfillment.get('fulfillments', [{}])[0].get("shipCode", ""),
                    "Must Arrive By Date": sofulfillment.get('fulfillments', [{}])[0].get("mustArriveByDate", ""),
                    "Update Date": sofulfillment.get('fulfillments', [{}])[0].get("updateDate", ""),
                    "Merge Type": sofulfillment.get('fulfillments', [{}])[0].get("mergeType", ""),
                    "Manifest Date": sofulfillment.get('fulfillments', [{}])[0].get("manifestDate", ""),
                    "Revised Delivery Date": sofulfillment.get('fulfillments', [{}])[0].get("revisedDeliveryDate", ""),
                    "Delivery City": sofulfillment.get('fulfillments', [{}])[0].get("deliveryCity", ""),
                    "Source System Id": sofulfillment.get("sourceSystemId", ""),
                    "IsDirect Ship": "",
                    "SSC": "",
                    "OIC Id": sofulfillment.get('fulfillments', [{}])[0].get("oicId", ""),
                    "Order Date": soheader.get("orderDate", "")
                }
                flat_list.append(base_row)
            except Exception as inner_e:
                print(f"[ERROR] OutputFormat inner loop at index {so_index}: {inner_e}")
                traceback.print_exc()
                continue

        if format_type == "export":
            return flat_list

        elif format_type == "grid":
            desired_order = [
                'BUID','PP Date','Sales Order Id','Fulfillment Id','Region Code','FoId','System Qty','Ship By Date',
                'LOB','Ship From Facility','Ship To Facility','Tax Regstrn Num','Address Line1','Postal Code','State Code',
                'City Code','Customer Num','Customer Name Ext','Country','Create Date','Ship Code','Must Arrive By Date',
                'Update Date','Merge Type','Manifest Date','Revised Delivery Date','Delivery City','Source System Id','IsDirect Ship',
                'SSC','Vendor Work Order Num','Channel Status Code','Ismultipack','Ship Mode','Is Otm Enabled',
                'SN Number','OIC Id', 'Order Date'
            ]
            rows = []
            for item in flat_list:
                reordered_values = [item.get(key) for key in desired_order]

                row = {
                    "columns": [{"value": val if val is not None else ""} for val in reordered_values]
                }

                rows.append(row)
                table_grid_output = tablestructural(rows, region)
                
            return table_grid_output

        return {"error": "Format type must be either 'grid' or 'export'"}
    except Exception as e:
        print(f"[ERROR] OutputFormat failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}

if __name__ == "__main__":
    region = "DAO"
    format_type = 'grid'
    sample_filters ={
            "Sales_Order_id": "8040047621,8040047674"
            # "1004543337,",
            # "foid": "7336030653629440001,7336030653629440001",
            # "Fullfillment Id": "262135,7336030653629440001",
            # "wo_id": "7360928459,57336030653629440001",
            # "Sales_order_ref": "REF123456",
            # "Order_create_date": "2025-07-15",
            # "Order_from_date" :"2025-05-01",
            # "Order_to_date" :"2025-05-12",
            # "ISMULTIPACK": "Yes",
            # "BUID": "202",
            # "Facility": "WH_BANGALORE",
            # "Manifest_ID": "MANI0001"
        }
    

    output = fieldValidation(sample_filters,format_type,region)
    flat_table = OutputFormat(output,format_type,region)
    
    print(json.dumps(flat_table,indent=2))

