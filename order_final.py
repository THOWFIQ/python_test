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

        soi = json.dumps(so_id)
       
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
               
        fetchfillmentids_query = fetch_getByFulfillmentids_query(fulfillment_id)
        fetchfillmentids_data = post_api(URL=path['FID'], query=fetchfillmentids_query, variables=None)
       
        if fetchfillmentids_data and fetchfillmentids_data.get('data'):
            combined_fullfillment_data['data']['getByFulfillmentids'] = fetchfillmentids_data['data'].get('getByFulfillmentids', {})

        sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
        sofulfillment_data = post_api(URL=path['SOPATH'], query=sofulfillment_query, variables=None)
        
        if sofulfillment_data and sofulfillment_data.get('data'):
            combined_fullfillment_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})

        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f"Fullfillment Function took {elapsed_time:.2f} seconds to complete.")
        
        return combined_fullfillment_data

    except Exception as e:
        print(f"Error in combined_fulfillment_fetch: {e}")
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
    results = chain.from_iterable(
        map(lambda entry: entry.get("data", {}).get("getBySalesorderids", {}).get("result", []), all_data)
    )

    fulfillments = chain.from_iterable(
        map(lambda result: result.get("fulfillment", []) if isinstance(result.get("fulfillment"), list) else [], results)
    )

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
            'Sales_Order_id': []
        }

        # # ---------- OrderDate  Block ----------
        if 'from' in primary_filters and 'to' in primary_filters :
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

                thread_fetch_and_store(combined_fulfillment_fetch, fulfillment_ids, region, filters, 'Fullfillment Id', result_map)

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

