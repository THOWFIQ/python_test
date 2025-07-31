from flask import request, jsonify
import requests
import httpx
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from itertools import chain

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

PRIMARY_FIELDS = {
    "Sales_Order_id", "wo_id", "Fullfillment Id", "foid", "order_date","from","to"
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
            response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False,timeout=180)
        else:
            response = httpx.post(URL, json={"query": query}, verify=False,timeout=180)
        return response.json()
    except Exception as e:
        print(f"[ERROR] post_api failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}

def combined_salesorder_fetch(so_id, region, filters):
    start_time = time.time()
    combined_salesorder_data = {'data': {}}
    try:
        path = getPath(region)

        # soi = {"salesorderIds": [so_id]}
        soi = json.dumps(so_id)
        print(f" Sales Order Data : {soi}")
        if so_id is not None:
            salesorder_query = fetch_salesorder_query(soi)
            salesorder = post_api(URL=path['FID'], query=salesorder_query, variables=None)

            if salesorder and salesorder.get('data') and 'getBySalesorderids' in salesorder['data']:
                combined_salesorder_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"Sales Orer Function took {elapsed_time:.2f} seconds to complete.")
        
        return combined_salesorder_data
    except Exception as e:
        print(f"Error in combined_salesorder_fetch: {e}")
        return {}

def combined_fulfillment_fetch(fulfillment_id, region, filters):
    start_time = time.time()
    combined_fullfillment_data = {'data': {}}
    try:
        path = getPath(region)        
        
        ffids = json.dumps(fulfillment_id)
        print(f" Fullfillment Data : {ffids}")
        # Fetch fulfillmentbyids data
        fetchfillmentids_query = fetch_getByFulfillmentids_query(ffids)
        fetchfillmentids_data = post_api(URL=path['FID'], query=fetchfillmentids_query, variables=None)

        if fetchfillmentids_data and fetchfillmentids_data.get('data'):
            combined_fullfillment_data['data']['getByFulfillmentids'] = fetchfillmentids_data['data'].get('getByFulfillmentids', {})

        # Fetch sofulfillment data
        # sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(ffids)
        # sofulfillment_data = post_api(URL=path['SOPATH'], query=sofulfillment_query, variables=None)

        # if sofulfillment_data and sofulfillment_data.get('data'):
        #     combined_fullfillment_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})

        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"Fullfillment Function took {elapsed_time:.2f} seconds to complete.")
        
        return combined_fullfillment_data

    except Exception as e:
        print(f"Error in combined_fulfillment_fetch: {e}")
        return {}

def combined_fulfillment_five_fetch(fulfillment_id, region, filters):
    start_time = time.time()
    combined_fullfillment_five_data = {'data': {}}
    try:
        path = getPath(region)        
        
        ffids = json.dumps(fulfillment_id)
        print(f" Fullfillment five Data : {ffids}")        

        # Fetch sofulfillment data
        sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(ffids)
        sofulfillment_data = post_api(URL=path['SOPATH'], query=sofulfillment_query, variables=None)

        if sofulfillment_data and sofulfillment_data.get('data'):
            combined_fullfillment_five_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})

        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"Fullfillment Function took {elapsed_time:.2f} seconds to complete.")
        
        return combined_fullfillment_five_data

    except Exception as e:
        print(f"Error in combined_fulfillment_fetch: {e}")
        return {}

def combined_foid_fetch(fo_id, region, filters):
    start_time = time.time()
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

        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"FOID Function took {elapsed_time:.2f} seconds to complete.")
        return combined_foid_data

    except Exception as e:
        print(f"Error in combined_foid_fetch: {e}")
        return {}


def combined_woid_fetch(wo_id, region, filters):
    start_time = time.time()
    combined_wo_data = {'data': {}}
    try:
        path = getPath(region)

        combined_wo_data = {'data': {}}

        wo_query = fetch_getByWorkorderids_query(wo_id)

        wo_data = post_api(URL=path['FID'], query=wo_query, variables=None)

        if wo_data and wo_data.get('data'):
            combined_wo_data['data']['getByWorkorderids'] = wo_data['data'].get('getByWorkorderids', {})

        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"Woid Function took {elapsed_time:.2f} seconds to complete.")
        return combined_wo_data

    except Exception as e:
        print(f"[ERROR] Error in combined_woid_fetch: {e}")
        return {}

def combined_OrderDate_fetch(orderFromDate, orderToDate, region, filters):
    start_time = time.time()
    combined_orderDate_data = {'data': {}}
    try:
        path = getPath(region)
        orderDate_query = fetch_getOrderDate_query(orderFromDate, orderToDate)

        orderDate_data = post_api(URL=path['SOPATH'], query=orderDate_query, variables=None)

        if orderDate_data and orderDate_data.get('data'):
            combined_orderDate_data['data']['getOrdersByDate'] = orderDate_data['data'].get('getOrdersByDate', {})
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"order date Function took {elapsed_time:.2f} seconds to complete.")
        return combined_orderDate_data

    except Exception as e:
        print(f"[ERROR] Error in combined_orderDate_data_fetch: {e}")
        return {}

def extract_fulfillment_ids_with_map(all_data):
    # Step 1: Extract all 'result' lists from each entry
    results = chain.from_iterable(
        map(lambda entry: entry.get("data", {}).get("getBySalesorderids", {}).get("result", []), all_data)
    )

    # Step 2: Extract all 'fulfillment' lists from each result
    fulfillments = chain.from_iterable(
        map(lambda result: result.get("fulfillment", []) if isinstance(result.get("fulfillment"), list) else [], results)
    )

    # Step 3: Extract all 'fulfillmentId's
    fulfillment_ids = list(
        map(lambda f: f.get("fulfillmentId"), filter(lambda f: f.get("fulfillmentId"), fulfillments))
    )

    return fulfillment_ids

def run_multithread_batches(fetch_func, ids, region, filters, batch_size=50, max_workers=30, delay_between_batches=0.5):
    start_time = time.time()
    all_results = []
    
    def wrapper(batch_ids):
        results = []
        
        with ThreadPoolExecutor(max_workers=min(len(batch_ids), max_workers)) as executor:
            # futures = [executor.submit(fetch_func, i, region, filters) for i in batch_ids]
            futures = [executor.submit(fetch_func, batch_ids, region, filters)]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                except Exception as e:
                    print(f"Error fetching ID in batch: {e}")
        return results

    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        print(f"\nProcessing batch {i//batch_size + 1} ({len(batch)} items)...")

        try:
            batch_results = wrapper(batch)
            all_results.extend(batch_results)
        except Exception as e:
            print(f"Error in batch {i//batch_size + 1}: {e}")

        time.sleep(delay_between_batches)

    end_time = time.time()
    print(f"Thread Function took {end_time - start_time:.2f} seconds to complete.")
    return all_results

def thread_fetch_and_store(fetch_func, id_list, region, filters, key_name, result_map, max_workers=50, batch_size=30):
    results = []

    def wrapper(batch):
        batch_results = []
        for single_id in batch:
            try:
                res = fetch_func(single_id, region, filters)
                if res:
                    batch_results.append(res)
            except Exception as e:
                print(f"Error in {key_name} for ID {single_id}: {e}")
        return batch_results

    # Create batches
    batches = [id_list[i:i + batch_size] for i in range(0, len(id_list), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(wrapper, batch) for batch in batches]
        for future in as_completed(futures):
            results.extend(future.result())

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

        result_map = {
            'Fullfillment Id': [],
            'Sales_Order_id': [],
            'foid': [],
            'wo_id': []
        }

        # ---------- Sales Order Block ----------
        if 'Sales_Order_id' in primary_filters: 
            # start_time = time.time()
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
                            result_map['Fullfillment Id'].extend(threadRes)

                        elif 'Fullfillment Id' not in filters:
                            threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                            result_map['Fullfillment Id'].extend(threadRes)

                    except Exception as e:
                        print(f"[ERROR] Fulfillment Thread from Sales: {e}")

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
                       
                        sales = fullfilData.get('data', {}).get('getFulfillmentsBysofulfillmentid', [])
                        if not sales:
                            raise ValueError("Missing or empty 'getFulfillmentsById' list.")
                        
                        sales_id = sales[0].get('salesOrderId')
                        if sales_id is None:
                            raise ValueError("Missing 'salesOrderId'.")

                    except Exception as e:
                        print(f"[ERROR] Parsing fullfilData: {e}")

                    try:
                        if 'Sales_Order_id' not in filters or sales_id in [filters['Sales_Order_id']]:
                            threadRes = threadFunction(combined_salesorder_fetch, [sales_id], format_type, region, filters)
                            result_map['Sales_Order_id'].extend(threadRes)

                    except Exception as e:
                        print(f"[ERROR] Sales from Fulfillment: {e}")

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
                        if not result :
                            raise ValueError("Missing or empty 'result' list.")

                        first_result = result[0]

                        sales = first_result.get('salesOrderId')
                        if sales is None:
                            raise ValueError("Missing 'salesOrderId'.")

                        fill = first_result.get('fulfillmentId')
                        if fill is None:
                            raise ValueError("Missing 'fulfillmentId'.")

                    except Exception as e:
                        print(f"[ERROR] Parsing FOData: {e}")

                    try:
                        threadRes = threadFunction(combined_salesorder_fetch, [sales], format_type, region, filters)
                        result_map['Sales_Order_id'].extend(threadRes)

                    except Exception as e:
                        print(f"[ERROR] Sales from FOID: {e}")

                    try:
                        threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                        result_map['Fullfillment Id'].extend(threadRes)

                    except Exception as e:
                        print(f"[ERROR] Fulfillment from FOID: {e}")

            except Exception as e:
                print(f"[ERROR] FOID thread block: {e}")

        # # ---------- WOID Block ----------
        if 'wo_id' in primary_filters and not any(k in primary_filters for k in ['Sales_Order_id', 'Fullfillment Id', 'foid']):
            print('sales and fill , fo not in')
            try:
                wo_ids = list(set(x.strip() for x in primary_filters['wo_id'].split(',') if x.strip()))
                threadRes = threadFunction(combined_woid_fetch, wo_ids, format_type, region, filters)
                result_map['wo_id'] = threadRes
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

                        sales_orders = first_result.get('salesOrder', {})

                        if not sales_orders:
                            raise ValueError("Missing or empty 'workOrders' list.")
                        soisid = sales_orders.get('salesOrderId')
                        if soisid is None:
                            raise ValueError("Missing 'woId'.")

                    except Exception as e:
                        print(f"[ERROR] Parsing salesData: {e}")


                    try:
                        threadRes = threadFunction(combined_salesorder_fetch, [soisid], format_type, region, filters)
                        result_map['Sales_Order_id'].extend(threadRes)

                    except Exception as e:
                        print(f"[ERROR] Sales from WOID: {e}")

                    try:
                        threadRes = threadFunction(combined_fulfillment_fetch, [fill], format_type, region, filters)
                        result_map['Fullfillment Id'].extend(threadRes)

                    except Exception as e:
                        print(f"[ERROR] Fulfillment from WOID: {e}")

            except Exception as e:
                print(f"[ERROR] WOID thread block: {e}")

        # # ---------- OrderDate  Block ----------
        if 'from' in primary_filters and 'to' in primary_filters and not any(k in primary_filters for k in ['Sales_Order_id', 'Fullfillment Id', 'foid', 'wo_id']):
            print("I'm in Order Date Part")
            start_time = time.time()
            try:
                from_date = primary_filters.get('from')
                to_date = primary_filters.get('to')                
                
                OrderDate_Response = combined_OrderDate_fetch(from_date, to_date, region, filters)
                resultData = OrderDate_Response.get('data', {}).get('getOrdersByDate', {}).get('result', [])
                
                SalesOrderIDs = [item.get("salesOrderId") for item in resultData if item.get("salesOrderId")]

                print("Total Sales Order IDs:", len(SalesOrderIDs))
               
                all_data = run_multithread_batches(
                    fetch_func=combined_salesorder_fetch,
                    ids=SalesOrderIDs,
                    region=region,
                    filters=filters,
                    batch_size=49,
                    max_workers=30,
                    delay_between_batches=0.5
                )

                result_map['Sales_Order_id'] = all_data
                
                fulfillment_ids = extract_fulfillment_ids_with_map(all_data)
                
                print("Total Fullfillment IDs:", len(fulfillment_ids))

                all_data = run_multithread_batches(
                    fetch_func=combined_fulfillment_fetch,
                    ids=fulfillment_ids,
                    region=region,
                    filters=filters,
                    batch_size=49,
                    max_workers=30,
                    delay_between_batches=0.5
                )

                all_data = run_multithread_batches(
                    fetch_func=combined_fulfillment_five_fetch,
                    ids=fulfillment_ids,
                    region=region,
                    filters=filters,
                    batch_size=5,
                    max_workers=30,
                    delay_between_batches=0.5
                )
                
                # thread_fetch_and_store(combined_fulfillment_fetch, fulfillment_ids, region, filters, 'Fullfillment Id', result_map)
            except Exception as e:
                print(f"[ERROR] order date thread block: {e}")
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Order Date Function {elapsed_time:.2f} seconds to complete.")
        return result_map

    except Exception as e:
        print(f"[FATAL ERROR] in fieldValidation: {e}")
        return {"status": "error", "message": "Unexpected failure during validation"}

def threadFunction(functionName, ids,  format_type, region, filters):

    result = []
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(functionName, id, region, filters) for id in ids]
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    result.append(res)
            except Exception as e:
                print(f"[ERROR] Thread Function fetch failed: {e}")
    return result

def OutputFormat(result_map, format_type=None,region=None):
    start_time = time.time()
    
    try:
        flat_list = []
        sales_orders = result_map.get("Sales_Order_id", [])
        fulfillments = result_map.get("Fullfillment Id", [])
        for so_index, so_entry in enumerate(sales_orders):
            try:
                if not isinstance(so_entry, dict):
                    continue
                so_data = so_entry.get("data", {})
                get_salesorders = so_data.get("getBySalesorderids", {})

                salesorder = get_salesorders.get("result", [{}])[0]

                fulfillment, sofulfillment = {}, {}
                if so_index < len(fulfillments):
                    fulfillment_entry = fulfillments[so_index]
                    if isinstance(fulfillment_entry, dict):
                        fulfillment_data = fulfillment_entry.get("data", {})
                        s_raw = fulfillment_data.get("getFulfillmentsBysofulfillmentid", [])

                        sofulfillment = s_raw[0] if s_raw else {}
                       
                base_row = {
                    "BUID": salesorder.get('salesOrder', {}).get('buid', ""),
                    "PP Date": "",
                    "Sales Order Id": salesorder.get('salesOrder', {}).get('salesOrderId', ""),
                    "Fulfillment Id": salesorder['fulfillment'][0]['fulfillmentId'] if salesorder.get('fulfillment') else "",
                    "Region Code": salesorder.get('salesOrder', {}).get('region', ""),
                    "FoId": salesorder['fulfillmentOrders'][0]['foId'] if salesorder.get('fulfillmentOrders') else "",
                    "woId": salesorder['workOrders'][0]['woId'] if salesorder.get('workOrders') else "",
                    "System Qty": "",
                    "Ship By Date": sofulfillment.get('fulfillments', [{}])[0].get('shipByDate', ""),
                    "LOB": "",
                    "Ship From Facility": salesorder['asnNumbers'][0]['shipFrom'] if salesorder.get('asnNumbers') else "",
                    "Ship To Facility": salesorder['asnNumbers'][0]['shipTo'] if salesorder.get('asnNumbers') else "",
                    "Facility": sofulfillment.get('fulfillments', [{}])[0].get('salesOrderLines', [{}])[0].get('facility', ""),
                    "ASN Number": salesorder['asnNumbers'][0]['snNumber'] if salesorder.get('asnNumbers') else "",
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
                    "Order Date": salesorder.get('salesOrder', {}).get('createDate', ""),
                }
                flat_list.append(base_row)
            except Exception as inner_e:
                print(f"[ERROR] OutputFormat inner loop at index {so_index}: {inner_e}")
                traceback.print_exc()
                continue
                        
        if len(flat_list) > 0:
            if format_type == "export":
                return flat_list

            elif format_type == "grid":
                desired_order = [
                    'BUID','PP Date','Sales Order Id','Fulfillment Id','Region Code','FoId','woId','System Qty','Ship By Date',
                    'LOB','Ship From Facility','Ship To Facility','Facility','ASN Number','Tax Regstrn Num','Address Line1','Postal Code','State Code',
                    'City Code','Customer Num','Customer Name Ext','Country','Create Date','Ship Code','Must Arrive By Date',
                    'Update Date','Merge Type','Manifest Date','Revised Delivery Date','Delivery City','Source System Id','IsDirect Ship',
                    'SSC','Vendor Work Order Num','Channel Status Code','Ismultipack','Ship Mode','Is Otm Enabled',
                    'OIC Id', 'Order Date'
                ]
                rows = []
                for item in flat_list:
                    reordered_values = [item.get(key) for key in desired_order]

                    row = {
                        "columns": [{"value": val if val is not None else ""} for val in reordered_values]
                    }

                    rows.append(row)
                    table_grid_output = tablestructural(rows, region) if rows else []
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"OUT PUT took {elapsed_time:.2f} seconds to complete.")
                return table_grid_output
        else:            
            Error_Message = {"Error Message": "No Data Found"}
            return Error_Message

        return {"error": "Format type must be either 'grid' or 'export'"}
    except Exception as e:
        print(f"[ERROR] OutputFormat failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}

