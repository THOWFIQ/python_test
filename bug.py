import os
import sys
import json
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add current directory to sys path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import all GraphQL queries and helpers
from queries import fetch_salesorder_query, tablestructural

# Load config
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)


# -----------------------
# GraphQL Client
# -----------------------
def execute_graphql_query(query, variables=None):
    endpoint = configPath.get("GraphQL_Endpoint")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {configPath.get('GraphQL_Token')}"
    }

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    try:
        response = httpx.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] GraphQL request failed: {e}")
        return None


# -----------------------
# Get by SalesOrderID Main
# -----------------------
def getbySalesOrderID(salesorderid: list, format_type: str = "grid", region: str = "DAO"):
    def fetch_and_flatten(salesid):
        query = fetch_salesorder_query(salesid)
        data = execute_graphql_query(query)

        if not data:
            print(f"[ERROR] No response for Sales Order ID: {salesid}")
            return None

        if "data" not in data or not data["data"]:
            print(f"[ERROR] 'data' missing in response for {salesid}")
            return None

        if "getBySalesorderids" not in data["data"] or not data["data"]["getBySalesorderids"]:
            print(f"[ERROR] 'getBySalesorderids' is missing or null for {salesid}")
            return None

        results = data["data"]["getBySalesorderids"].get("result")
        if not results or not isinstance(results, list):
            print(f"[INFO] No records found for Sales Order ID: {salesid}")
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

    print(f"[INFO] Fetching {len(salesorderid)} sales order(s)...")

    # Use ThreadPool for concurrent GraphQL calls
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_sid = {executor.submit(fetch_and_flatten, sid): sid for sid in salesorderid}
        for future in as_completed(future_to_sid):
            sid = future_to_sid[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                print(f"[ERROR] Exception while processing {sid}: {exc}")

    if format_type.lower() == "export":
        return json.dumps(results, indent=2)
    else:
        structured = tablestructural(results, region.upper())
        return json.dumps(structured, indent=2)


# -----------------------
# Run for CLI testing
# -----------------------
if __name__ == "__main__":
    salesorderIds = ["1004452326"]  # Replace with your test IDs
    region = "EMEA"
    format_type = "grid"  # grid or export

    result = getbySalesOrderID(salesorderid=salesorderIds, format_type=format_type, region=region)
    print(result)
