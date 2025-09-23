from flask import Flask, request, jsonify
import nest_asyncio
import asyncio
import aiohttp
import requests
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from functools import reduce
import traceback
import time
import os
import sys
import json

nest_asyncio.apply()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries_new import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

SequenceValue = []
ValidCount  = []

def newmainfunction(filters, format_type, region):
    
    region = region.upper()
    path = getPath(region)

    graphql_request = []

    if "Fullfillment Id" in filters:
        REGION = ""
        fulfillment_key = "Fullfillment Id"
        matched = False
        uniqueFullfillment_ids = ",".join(sorted(set(filters[fulfillment_key].split(','))))
        filters[fulfillment_key] = uniqueFullfillment_ids

        if filters.get(fulfillment_key):
            fulfillment_ids = list(map(str.strip, filters[fulfillment_key].split(",")))

            for ffid in fulfillment_ids:
                graphql_request.append({
                        "url": path['SALESFULLFILLMENT'],
                        "query": fetch_Fullfillment_query(ffid)
                    })
                print(f"Sequence of FULLFILLMENT DATA : {ffid}")

    if "Sales_Order_id" in filters:
        REGION = ""
        salesOrder_key = "Sales_Order_id"
        matched = False
        uniqueSalesOrder_ids = ",".join(sorted(set(filters[salesOrder_key].split(','))))
        filters[salesOrder_key] = uniqueSalesOrder_ids

        if filters.get(salesOrder_key):
            salesorder_ids = list(map(str.strip, filters[salesOrder_key].split(",")))
            print("\n")
            print(f"length proceed : {len(salesorder_ids)}")
            print("\n")
            for soid_chunk in chunk_list(salesorder_ids,10):
                graphql_request.append({
                        "url": path['SALESFULLFILLMENT'],
                        "query": fetch_salesOrder_query(soid_chunk)
                    })
                print(f"Sequence of SALES ORDER DATA : {soid_chunk}")

    results = asyncio.run(run_all(graphql_request))

    return results

async def fetch_graphql(session, url, query):
    async with session.post(url, json={"query": query}) as response:
        return await response.json()

async def run_all(graphql_request):
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_graphql(session, req["url"], req["query"])
            for req in graphql_request
        ]
        results = await asyncio.gather(*tasks)
        return results

def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    try:
        def extract_sales_order(data):
            if not data or not isinstance(data, dict):
                return None

            soids_data = data.get("getSalesOrderBySoids")
            if soids_data:
                sales_orders = soids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders

            ffids_data = data.get("getSalesOrderByFfids")
            if ffids_data:
                sales_orders = ffids_data.get("salesOrders")
                if sales_orders:
                    return sales_orders

            return None

        flat_list = []
        ValidCount = []
        
        for item in result_map:            
            data = item.get("data")
            
            if not data:
                continue

            soids_data = data.get("getSalesOrderBySoids")
            ffids_data = data.get("getSalesOrderByFfids")
            
            sales_orders = extract_sales_order(data)
            if not sales_orders or len(sales_orders) == 0:
                continue

            # ✅ FIX: iterate through ALL sales orders, not just the first one
            for so in sales_orders:
                if filtersValue:
                    if soids_data and not ffids_data:
                        sales_order_id = safe_get(so, ['salesOrderId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(sales_order_id)
                            print("Appended sales_order_id:", sales_order_id)
                            print("Current ValidCount:", ValidCount)
                    elif ffids_data and not soids_data:
                        fulfillment_id = safe_get(so, ['fulfillments', 'fulfillmentId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(fulfillment_id)
                            print("Appended fulfillment_id:", fulfillment_id)
                            print("Current ValidCount:", ValidCount)
                else:
                    print("Both soids_data and ffids_data present, skipping append")
                    # pass
                
                if region and region.upper() != safe_get(so, ['region'], "").upper():
                    continue

                shipping_addr = pick_address_by_type(so, "SHIPPING")
                billing_addr = pick_address_by_type(so, "BILLING")
                shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

                lob_list = list(filter(
                    lambda lob: lob and lob.strip() != "",
                    [
                        safe_get(line, ['lob'])
                        for fulfillment in safe_get(so, ['fulfillments']) or []
                        for line in safe_get(fulfillment, ['salesOrderLines']) or []
                    ]
                ))
                lob = ", ".join(lob_list)

                facility_list = list(filter(
                    lambda x: x and x.strip() != "",
                    [
                        safe_get(line, ['facility'])
                        for fulfillment in safe_get(so, ['fulfillments']) or []
                        for line in safe_get(fulfillment, ['salesOrderLines']) or []
                    ]
                ))
                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f and f.strip()))

                def get_status_date(code):
                    status_code = safe_get(so, ['fulfillments', 'sostatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(so, ['fulfillments', 'sostatus', 0, 'statusDate']))
                    return ""
                
                row = {
                    "Fulfillment ID": safe_get(so, ['fulfillments', 'fulfillmentId']),
                    "BUID": safe_get(so, ['buid']),
                    "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                    "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "LOB": lob,
                    "Sales Order ID": safe_get(so, ['salesOrderId']),
                    "Agreement ID": safe_get(so, ['agreementId']),
                    "Amount": safe_get(so, ['totalPrice']),
                    "Currency Code": safe_get(so, ['currency']),
                    "Customer Po Number": safe_get(so, ['poNumber']),
                    "Delivery City": safe_get(so, ['fulfillments', 'deliveryCity']),
                    "DOMS Status": safe_get(so, ['fulfillments', 'sostatus', 0, 'sourceSystemStsCode']),
                    "Dp ID": safe_get(so, ['dpid']),
                    "Fulfillment Status": safe_get(so, ['fulfillments', 'sostatus', 0, 'fulfillmentStsCode']),
                    "Merge Type": safe_get(so, ['fulfillments', 'mergeType']),
                    "InstallInstruction2": get_install_instruction2_id(so),
                    "PP Date": get_status_date("PP"),
                    "IP Date": get_status_date("IP"),
                    "MN Date": get_status_date("MN"),
                    "SC Date": get_status_date("SC"),
                    "Location Number": safe_get(so, ['locationNum']),
                    "OFS Status Code": safe_get(so, ['fulfillments', 'sostatus', 0, 'sourceSystemStsCode']),
                    "OFS Status": safe_get(so, ['fulfillments', 'sostatus', 0, 'fulfillmentStsCode']),
                    "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "ShippingContactName": shipping_contact_name,
                    "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                    "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                    "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShipToPhone": (listify(shipping_addr.get("phone", []))[0].get("phoneNumber", "")
                                    if shipping_addr and listify(shipping_addr.get("phone", [])) else ""),
                    "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                    "Order Age": safe_get(so, ['orderDate']),
                    "Order Amount usd": safe_get(so, ['rateUsdTransactional']),
                    "Rate Usd Transactional": safe_get(so, ['rateUsdTransactional']),
                    "Sales Rep Name": safe_get(so, ['salesrep', 0, 'salesRepName']),
                    "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Source System Status": safe_get(so, ['fulfillments', 'sostatus', 0, 'sourceSystemStsCode']),
                    "Tie Number": safe_get(so, ['fulfillments', 'salesOrderLines', 0, 'soLineNum']),
                    "Si Number": safe_get(so, ['fulfillments', 'salesOrderLines', 0, 'siNumber']),
                    "Req Ship Code": safe_get(so, ['fulfillments', 'shipCode']),
                    "Reassigned IP Date": safe_get(so, ['fulfillments', 'sostatus', 0, 'sourceSystemStsCode']),
                    "Payment Term Code": safe_get(so, ['fulfillments', 'paymentTerm']),
                    "Region Code": safe_get(so, ['region']),
                    "FO ID": safe_get(so, ['fulfillments', 'fulfillmentOrder', 0, 'foId']),
                    "System Qty": safe_get(so, ['fulfillments', 'systemQty']),
                    "Ship By Date": safe_get(so, ['fulfillments', 'shipByDate']),
                    "Facility": facility,
                    "Tax Regstrn Num": safe_get(so, ['fulfillments', 'address', 0, 'taxRegstrnNum']),
                    "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
                    "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
                    "Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Ship Code": safe_get(so, ['fulfillments', 'shipCode']),
                    "Must Arrive By Date": dateFormation(safe_get(so, ['fulfillments', 'mustArriveByDate'])),
                    "Manifest Date": dateFormation(safe_get(so, ['fulfillments', 'manifestDate'])),
                    "Revised Delivery Date": dateFormation(safe_get(so, ['fulfillments', 'revisedDeliveryDate'])),
                    "Source System ID": safe_get(so, ['sourceSystemId']),
                    "OIC ID": safe_get(so, ['fulfillments', 'oicId']),
                    "Order Date": dateFormation(safe_get(so, ['orderDate'])),
                    "Order Type": dateFormation(safe_get(so, ['orderType']))
                }

                flat_list.append(row)

        count_valid = len(ValidCount)

        if not flat_list:
            return {"error": "No Data Found"}
       
        if len(flat_list) > 0:
            if format_type == "export":
                if filtersValue:
                    data = []
                    count =  {"Count ": count_valid}
                    data.append(count)
                    data.append(flat_list)
                    ValidCount.clear()
                    return data
                else:
                    return flat_list

            elif format_type == "grid":
                
                desired_order = [
                    "Fulfillment ID","BUID","BillingCustomerName","CustomerName","LOB","Sales Order ID","Agreement ID",
                    "Amount","Currency Code","Customer Po Number","Delivery City","DOMS Status","Dp ID","Fulfillment Status",
                    "Merge Type","InstallInstruction2","PP Date","IP Date","MN Date","SC Date","Location Number","OFS Status Code",
                    "OFS Status","ShippingCityCode","ShippingContactName","ShippingCustName","ShippingStateCode","ShipToAddress1",
                    "ShipToAddress2","ShipToCompany","ShipToPhone","ShipToPostal","Order Age","Order Amount usd","Rate Usd Transactional",
                    "Sales Rep Name","Shipping Country","Source System Status","Tie Number","Si Number","Req Ship Code",
                    "Reassigned IP Date","Payment Term Code","Region Code","FO ID","System Qty","Ship By Date","Facility",
                    "Tax Regstrn Num","State Code","City Code","Customer Num","Customer Name Ext","Country","Ship Code",
                    "Must Arrive By Date","Manifest Date","Revised Delivery Date","Source System ID","OIC ID","Order Date","Order Type"
                ]
                
                rows = []
                for item in flat_list:
                    reordered_values = [item.get(key, "") for key in desired_order]
                    row = {"columns": [{"value": val if val is not None else ""} for val in reordered_values]}
                    rows.append(row)
                table_grid_output = tablestructural(rows,region) if rows else []
                
                if filtersValue:
                    table_grid_output["Count"] = count_valid
                ValidCount.clear()
                return table_grid_output

    except Exception as e:
        return {"error": str(e)}

def safe_get(data, keys, default=""):
    if data is None:
        return default

    for key in keys:
        if isinstance(data, dict):
            if key == "fulfillments":
                val = data.get(key)
                if isinstance(val, list):
                    data = val[0]
                else:
                    data = val
            else:
                data = data.get(key, default)
        elif isinstance(data, list):
            try:
                index = int(key)
                if 0 <= index < len(data):
                    data = data[index]
                else:
                    return default
            except (ValueError, TypeError):
                return default
        else:
            return default

        if data is None:
            return default
    return data

def dateFormation(unformatedDate):
    if unformatedDate not in [None, "", "null"]:
        return unformatedDate.split('.')[0]
    else:
        return ""

def chunk_list(data_list, chunk_size):
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]

def listify(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def pick_address_by_type(salesheader_entry: Dict, contact_type: str) -> Dict:
    
    addresses = salesheader_entry.get("address", [])
    addresses = listify(addresses)
    for addr in addresses:
        contacts = listify(addr.get("contact", []))
        for c in contacts:
            if isinstance(c, dict) and c.get("contactType") == contact_type:
                return addr
    return {}

def get_install_instruction2_id(fulfillment_entry: Dict) -> str:
    
    fulfills = listify(fulfillment_entry.get("fulfillments", []))
    if not fulfills:
        return ""
    lines = listify(fulfills[0].get("salesOrderLines", []))
    for line in lines:
        instrs = listify(line.get("specialinstructions", []))
        for instr in instrs:
            if instr.get("specialInstructionType") == "INSTALL_INSTR2":
                return str(instr.get("specialInstructionId", ""))
    return ""

def getPath(region):
