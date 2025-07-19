import os
import json
from graphqlqueries import (
    fetch_salesorder_query,
    fetch_workOrderId_query,
    fetch_fulfillment_query,
    fetch_foid_query,
    fetch_getAsn_query,
    fetch_getAsnbySn_query,
    fetch_getByFulfillmentids_query,
    fetch_getOrderDate_query
)
from utility import post_api  # This must handle POSTing GraphQL queries with optional variables


def load_config():
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
    with open(config_path, "r") as f:
        return json.load(f)


def get_path(region, path, configPath):
    region = region.upper()
    path = path.upper()
    return {
        ("FID", "DAO"): configPath.get("Linkage_DAO"),
        ("FID", "APJ"): configPath.get("Linkage_APJ"),
        ("FID", "EMEA"): configPath.get("Linkage_EMEA"),
        ("FOID", "DAO"): configPath.get("FM_Order_DAO"),
        ("FOID", "APJ"): configPath.get("FM_Order_EMEA_APJ"),
        ("FOID", "EMEA"): configPath.get("FM_Order_EMEA_APJ"),
        ("SOPATH", "DAO"): configPath.get("SO_Header_DAO"),
        ("SOPATH", "APJ"): configPath.get("SO_Header_EMEA_APJ"),
        ("SOPATH", "EMEA"): configPath.get("SO_Header_EMEA_APJ"),
        ("WOID", "DAO"): configPath.get("WO_Details_DAO"),
        ("WOID", "APJ"): configPath.get("WO_Details_EMEA_APJ"),
        ("WOID", "EMEA"): configPath.get("WO_Details_EMEA_APJ"),
        ("FFBOM", "DAO"): configPath.get("FM_BOM_DAO"),
        ("FFBOM", "APJ"): configPath.get("FM_BOM_EMEA_APJ"),
        ("FFBOM", "EMEA"): configPath.get("FM_BOM_EMEA_APJ"),
    }.get((path, region), None)


def get_by_combination(filters: dict, region: str, format_type: str = "export"):
    config = load_config()
    data = []

    # Paths
    sopath = get_path(region, "SOPATH", config)
    wopath = get_path(region, "WOID", config)
    fidpath = get_path(region, "FID", config)
    foidpath = get_path(region, "FOID", config)

    # Mappings from filters to GraphQL queries
    if sales_order_id := filters.get("Sales_Order_id"):
        query = fetch_salesorder_query(sales_order_id)
        response = post_api(sopath, query)
        if response and response.get("data"):
            data.append(response["data"])

    if wo_id := filters.get("wo_id"):
        query = fetch_workOrderId_query(wo_id)
        response = post_api(wopath, query)
        if response and response.get("data"):
            data.append(response["data"])

    if fulfillment_id := filters.get("Fullfillment Id"):
        query = fetch_fulfillment_query()
        response = post_api(sopath, query, {"fulfillment_id": fulfillment_id})
        if response and response.get("data"):
            data.append(response["data"])

    if foid := filters.get("foid"):
        query = fetch_foid_query(foid)
        response = post_api(foidpath, query)
        if response and response.get("data"):
            data.append(response["data"])

    if manifest_id := filters.get("Manifest ID"):
        query = fetch_getAsn_query(manifest_id)
        response = post_api(fidpath, query)
        if response and response.get("data"):
            data.append(response["data"])

    if sn_number := filters.get("SN Number"):
        query = fetch_getAsnbySn_query(sn_number)
        response = post_api(fidpath, query)
        if response and response.get("data"):
            data.append(response["data"])

    if order_date := filters.get("order_date"):
        # you can assume date format is "YYYY-MM-DD to YYYY-MM-DD"
        try:
            from_date, to_date = order_date.split(" to ")
            query = fetch_getOrderDate_query(from_date.strip(), to_date.strip())
            response = post_api(sopath, query)
            if response and response.get("data"):
                data.append(response["data"])
        except Exception as e:
            print("Invalid order_date range format:", e)

    # -----------------------------------
    # Optional fields: Post-fetch filters
    # -----------------------------------
    def match_optional_fields(entry):
        for key, expected in filters.items():
            if key == "ISMULTIPACK":
                if not any(line.get("ismultipack") == expected for line in entry.get("woLines", [])):
                    return False
            elif key == "BUID" and entry.get("buid") != expected:
                return False
            elif key == "Facility":
                facilities = (entry.get("shipFromFacility"), entry.get("shipToFacility"))
                if expected not in facilities:
                    return False
            elif key == "Order create_date":
                if entry.get("createDate") != expected:
                    return False
            elif key == "Sales_order_ref":
                if entry.get("soHeaderRef") != expected:
                    return False
        return True

    # Flatten + filter
    flat_data = []
    for item in data:
        if isinstance(item, dict):
            for key, val in item.items():
                if isinstance(val, list):
                    for rec in val:
                        if match_optional_fields(rec):
                            flat_data.append(rec)
                elif isinstance(val, dict):
                    if match_optional_fields(val):
                        flat_data.append(val)

    return flat_data
