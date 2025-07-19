from flask import request, jsonify
import requests
import httpx
import json
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# Load Config
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

# API Paths
FID = configPath['Linkage_DAO']
FOID = configPath['FM_Order_DAO']
SOPATH = configPath['SO_Header_DAO']
WOID = configPath['WO_Details_DAO']
FFBOM = configPath['FM_BOM_DAO']


def post_api(URL, query, variables=None):
    response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()


def extract_and_merge(records):
    output = []

    # Just for demonstration; ideally values should come from `records` after parsing real GraphQL response.
    template = {
        "Address Line1": "123 Main Street",
        "BUID": 202,
        "Channel Status Code": "400",
        "City Code": "NYC",
        "Country": "USA",
        "Create Date": "2025-06-04T09:06:35.538557",
        "Customer NameExt": "John A. Doe",
        "Customer Num": "C12345",
        "Delivery City": "",
        "FoId": "7336030653629440001",
        "Fulfillment Id": "262135",
        "Is Otm Enabled": "Y",
        "IsDirect Ship": "",
        "Ismultipack": "",
        "LOB": "",
        "Manifest Date": "",
        "Merge Type": "",
        "Must Arrive By Date": "",
        "OIC Id": "1eaf6101-b42b-4bc9-9036-98ee27442039",
        "Order Date": "2024-12-19T12:34:56",
        "PP Date": "",
        "Postal Code": "10001",
        "Region Code": "England",
        "Revised Delivery Date": "",
        "SN Number": "",
        "SSC": "SSC001",
        "Sales Order Id": "1004543337",
        "Ship By Date": "",
        "Ship Code": "",
        "Ship From Facility": "EMFC",
        "Ship Mode": "A",
        "Ship To Facility": "COV",
        "Source System Id": "OMEGA",
        "State Code": "NY",
        "System Qty": "",
        "Tax Regstrn Num": "TX987654",
        "Update Date": "2025-06-04T09:06:35.542619",
        "Vendor Work Order Num": ""
    }

    for sn, wo in [("CHVSN20250611110608251", "7360970693"), ("C2ZCWA24JXSZ", "7360928459"), ("ORI5H54EGUKU", "7360928459")]:
        record = template.copy()
        record["SN Number"] = sn
        record["Vendor Work Order Num"] = wo
        output.append(record)

    return output


def tablestructural(data, IsPrimary):
    all_keys = set(key for row in data for key in row.keys())

    def group_type(key):
        if "Id" in key or "Num" in key:
            return "ID"
        if "Date" in key:
            return "Date"
        if "Address" in key:
            return "Address"
        if "Facility" in key:
            return "Facility"
        if "Code" in key:
            return "Code"
        if "Is" in key:
            return "Flag"
        if "Mode" in key:
            return "Mode"
        if "Type" in key:
            return "Type"
        return "Other"

    primary_fields = ["BUID", "PP Date", "Sales Order Id", "Fulfillment Id", "System Qty", "Ship By Date", "LOB", "Ship From Facility", "Ship To Facility"]

    columns = [
        {
            "checked": key in primary_fields,
            "group": group_type(key),
            "isPrimary": key in primary_fields,
            "sortBy": "ascending",
            "value": key
        } for key in sorted(all_keys)
    ]

    data_rows = [{"columns": [{"value": row.get(col["value"], "")} for col in columns]} for row in data]

    return {
        "columns": columns,
        "data": data_rows
    }


def getbySalesOrderID(salesorderid, format_type, region, filters=None):
    extracted_data = extract_and_merge(records=None)  # replace None with actual data logic if needed

    if format_type == "export":
        return json.dumps(extracted_data, indent=2)
    elif format_type == "grid":
        return tablestructural(data=extracted_data, IsPrimary=region)
    else:
        return {"error": "Invalid format type"}


if __name__ == "__main__":
    export_output = getbySalesOrderID(
        salesorderid=["1004543337"],
        format_type="export",
        region="EMEA",
        filters={}
    )
    print("Export Output")
    print(export_output)

    grid_output = getbySalesOrderID(
        salesorderid=["1004543337"],
        format_type="grid",
        region="EMEA",
        filters={}
    )
    print("\nGrid Output")
    print(json.dumps(grid_output, indent=2))
