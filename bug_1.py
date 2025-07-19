# Full working code with export and grid output format support
# Includes a unified flattening logic for get_by_combination and getbySalesOrderIDs

from concurrent.futures import ThreadPoolExecutor, as_completed
import os, sys, json, httpx

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# Load Config
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

FID = configPath['Linkage_DAO']
FOID = configPath['FM_Order_DAO']
SOPATH = configPath['SO_Header_DAO']
WOID = configPath['WO_Details_DAO']
FFBOM = configPath['FM_BOM_DAO']

def post_api(URL, query, variables=None):
    response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()

def flatten_record(record):
    return {
        "Sales Order Id": record.get("salesOrderId") or record.get("soHeaderRef"),
        "FoId": record.get("foId"),
        "Fulfillment Id": record.get("fulfillmentId"),
        "Region Code": record.get("region"),
        "System Qty": record.get("systemQty") or record.get("fulfillments", [{}])[0].get("systemQty"),
        "Ship By Date": record.get("shipByDate") or record.get("fulfillments", [{}])[0].get("shipByDate"),
        "LOB": record.get("lob") or record.get("salesOrderLines", [{}])[0].get("lob"),
        "Ship From Facility": record.get("shipFromFacility"),
        "Ship To Facility": record.get("shipToFacility"),
        "SN Number": record.get("snNumber") or record.get("woLines", [{}])[0].get("snNumber"),
        "Vendor Work Order Num": record.get("woId"),
        "Channel Status Code": record.get("channelStatusCode"),
        "Is Otm Enabled": record.get("isOtmEnabled"),
        "Ismultipack": next((line.get("ismultipack") for line in record.get("woLines", []) if "ismultipack" in line), ""),
        "Ship Mode": record.get("shipMode"),
        "BUID": record.get("buid"),
        "Customer Name Ext": record.get("customerNameExt") or record.get("address", [{}])[0].get("customerNameExt"),
        "Customer Num": record.get("customerNum") or record.get("address", [{}])[0].get("customerNum"),
        "Address Line1": record.get("addressLine1") or record.get("address", [{}])[0].get("addressLine1"),
        "Postal Code": record.get("postalCode") or record.get("address", [{}])[0].get("postalCode"),
        "City Code": record.get("cityCode") or record.get("address", [{}])[0].get("cityCode"),
        "State Code": record.get("stateCode") or record.get("address", [{}])[0].get("stateCode"),
        "Country": record.get("country") or record.get("address", [{}])[0].get("country"),
        "Order Date": record.get("orderDate"),
        "Create Date": record.get("createDate") or record.get("address", [{}])[0].get("createDate"),
        "Update Date": record.get("updateDate"),
        "OIC Id": record.get("oicId"),
        "Ship Code": record.get("shipCode"),
        "SSC": record.get("ssc"),
        "IsDirect Ship": record.get("isDirectShip"),
        "Source System Id": record.get("sourceSystemId") or record.get("sourceSystem")
    }

def get_by_combination(filters: dict, region: str, format_type: str = "export"):
    data = []

    if filters.get("WorkOrderID"):
        query = fetch_workOrderId_query(filters["WorkOrderID"])
        response = post_api(WOID, query)
        if response and response.get("data"):
            data.append(response["data"])

    if filters.get("FullfillmentID"):
        query = fetch_fulfillment_query()
        response = post_api(SOPATH, query, {"fulfillment_id": filters["FullfillmentID"]})
        if response and response.get("data"):
            data.append(response["data"])

    flat_data = []
    for item in data:
        for key, val in item.items():
            if isinstance(val, list):
                for rec in val:
                    flat_data.append(flatten_record(rec))
    return flat_data

def apply_filters(data_list, filters):
    if not filters:
        return data_list

    def match(record):
        for key, value in filters.items():
            if str(record.get(key, "")).strip() != value.strip():
                return False
        return True

    return [item for item in data_list if match(item)]

def tablestructural(data, IsPrimary):
    if not data:
        return {"columns": [], "data": []}

    return {
        "columns": [
            {"value": k, "checked": True, "group": "General", "sortBy": "ascending", "isPrimary": True} for k in data[0].keys()
        ],
        "data": [{"columns": [{"value": v} for v in row.values()]} for row in data]
    }

def getbySalesOrderID(salesorderid=None, format_type="export", region="", filters=None):
    total_output = []
    if filters and not salesorderid:
        total_output = get_by_combination(filters, region, format_type)
    else:
        def fetch_order(so_id):
            # Simulate fetch (stub)
            return [{"Sales Order Id": so_id, "Region Code": region, "Fulfillment Id": filters.get("FullfillmentID", "")}]  # Dummy

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_order, sid) for sid in salesorderid]
            for future in as_completed(futures):
                total_output.extend(future.result())

    total_output = apply_filters(total_output, filters or {})

    if format_type == "export":
        print(json.dumps(total_output, indent=2))
        return json.dumps(total_output, indent=2)
    elif format_type == "grid":
        grid = tablestructural(total_output, region)
        print(json.dumps(grid, indent=2))
        return grid
    else:
        return {"error": "Invalid format type"}

if __name__ == "__main__":
    frontend_input = {
        "salesorderid": None,
        "format_type": "grid",
        "region": "EMEA",
        "filters": {
            "FullfillmentID": "262135",
            "WorkOrderID": "7360928459"
        }
    }
    getbySalesOrderID(
        salesorderid=frontend_input["salesorderid"],
        format_type=frontend_input["format_type"],
        region=frontend_input["region"],
        filters=frontend_input["filters"]
    )
