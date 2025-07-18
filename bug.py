if __name__ == "__main__":
    salesorderIds=["1004452326"]
    region = "EMEA"
    # salesorderIds=["1004452326", "1004543337"]
    # cleaned = fetch_and_clean()
    # print(cleaned)
    format_type='grid' #grid/export
    getbySalesOrderID(salesorderid=salesorderIds,format_type=format_type,region=region)


import json
import os
import sys
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add current folder to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import query builders and utilities
from queries import (
    fetch_salesorder_query,
    tablestructural,
    get_path
)

# Load config on startup
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

# Disable SSL warnings (for dev/test environments only)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def post_api(URL, query, variables=None):
    try:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = httpx.post(URL, json=payload, verify=False)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error calling GraphQL API: {e}")
        return None


def getbySalesOrderIDs(salesorderid, format_type, region):
    URL = get_path(region, "SOPATH", configPath)
    query = fetch_salesorder_query(",".join(salesorderid))
    result = post_api(URL, query)
    return result


def getbySalesOrderID(salesorderid, format_type, region):
    results = []

    def fetch_and_flatten(salesid):
        data = getbySalesOrderIDs([salesid], format_type, region)
    
        if not data:
            print(f"[ERROR] No response received for Sales Order ID: {salesid}")
            return None
    
        if "data" not in data or data["data"] is None:
            print(f"[ERROR] 'data' key missing or null in response for: {salesid}")
            return None
    
        if "getBySalesorderids" not in data["data"] or data["data"]["getBySalesorderids"] is None:
            print(f"[ERROR] 'getBySalesorderids' is missing or null for: {salesid}")
            return None
    
        results = data["data"]["getBySalesorderids"].get("result")
        if not results or not isinstance(results, list):
            print(f"[INFO] No results found for Sales Order ID: {salesid}")
            return None
    
        record = results[0]
    
        return {
            "Sales Order Id": record.get("salesOrder", {}).get("salesOrderId"),
            "BUID": record.get("salesOrder", {}).get("buid"),
            "Region Code": record.get("salesOrder", {}).get("region"),
            "Fulfillment Id": record.get("fulfillment", {}).get("fulfillmentId"),
            "FoId": record.get("fulfillmentOrders", [{}])[0].get("foId") if record.get("fulfillmentOrders") else None,
            "WO ID": record.get("workOrders", [{}])[0].get("woId") if record.get("workOrders") else None,
            "SN Number": record.get("asnNumbers", [{}])[0].get("snNumber") if record.get("asnNumbers") else None,
        }


    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_and_flatten, sid) for sid in salesorderid]
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    if format_type == 'grid':
        return tablestructural(results, region)
    elif format_type == 'export':
        return results
    else:
        return {"error": "Invalid format_type provided. Use 'grid' or 'export'"}


# Local test block
if __name__ == "__main__":
    salesorderIds = ["1004452326"]  # Replace with multiple IDs if needed
    region = "EMEA"
    format_type = "grid"  # or 'export'

    output = getbySalesOrderID(salesorderid=salesorderIds, format_type=format_type, region=region)
    print(json.dumps(output, indent=2) if isinstance(output, dict) else output)
