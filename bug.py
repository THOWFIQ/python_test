import os
import sys
import json
import nest_asyncio
import asyncio
import aiohttp
import traceback
from typing import List, Dict, Optional

nest_asyncio.apply()

# Make sure python path includes this file's dir if needed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# import all graphql query builders (your provided query file)
from graphqlQueries_new import *  # fetch_salesOrder_query, fetch_Fullfillment_query, fetch_workOrder_query, fetch_keysphereSalesorder_query, fetch_keysphereFullfillment_query, fetch_keysphereWorkorder_query

# load config
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

SequenceValue = []
ValidCount  = []

# ---------- Async HTTP GraphQL ----------
async def fetch_graphql(session, url, query):
    async with session.post(url, json={"query": query}) as response:
        try:
            return await response.json()
        except Exception:
            # sometimes response may be text
            text = await response.text()
            try:
                return json.loads(text)
            except Exception:
                return {"error": "invalid_json_response", "text": text}

async def run_all(graphql_request):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_graphql(session, req["url"], req["query"]) for req in graphql_request]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return results

# ---------- Helpers ----------
def safe_get(data, keys, default=""):
    if data is None:
        return default

    for key in keys:
        if isinstance(data, dict):
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
        return str(unformatedDate).split('.')[0]
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

def pick_address_by_type(so, contact_type):
    addresses = so.get("address", []) if isinstance(so, dict) else []
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
        instrs = listify(line.get("specialinstructions", [])) or listify(line.get("specialInstructions", []))
        for instr in instrs:
            if instr.get("specialInstructionType") == "INSTALL_INSTR2":
                return str(instr.get("specialInstructionId", ""))
    return ""

def getPath(region):
    try:
        region = (region or "").upper()
        if region == "EMEA":
            return {
                "FID": configPath['Linkage_EMEA'],
                "SALESFULLFILLMENT": configPath['SALES_ORDER_EMEA'],
                "WORKORDER": configPath['WORK_ORDER_EMEA']
            }
        elif region == "APJ":
            return {
                "FID": configPath['Linkage_APJ'],
                "SALESFULLFILLMENT": configPath['SALES_ORDER_APJ'],
                "WORKORDER": configPath['WORK_ORDER_APJ']
            }
        elif region in ["DAO", "AMER", "LA"]:
            return {
                "FID": configPath['Linkage_DAO'],
                "SALESFULLFILLMENT": configPath['SALES_ORDER_DAO'],
                "WORKORDER": configPath['WORK_ORDER_DAO']
            }
    except Exception as e:
        print(f"[ERROR] getPath failed: {e}")
        traceback.print_exc()
        return {}

# ---------- Main function: prepare requests, call FID to relate WOs ----------
def newmainfunction(filters, format_type, region):
    """
    filters: dict with possible keys: "Fullfillment Id", "Sales_Order_id", "wo_id"
    format_type: "export" | "grid" | None
    region: region string
    """
    region = (region or "").upper()
    path = getPath(region)
    graphql_request = []

    # track original requested ids to avoid duplicate follow-ups later
    original_salesorder_ids = set()
    original_fulfillment_ids = set()
    original_workorder_ids = set()

    # prepare initial requests
    if "Fullfillment Id" in filters:
        fulfillment_key = "Fullfillment Id"
        uniqueFullfillment_ids = ",".join(sorted(set(filters[fulfillment_key].split(','))))
        filters[fulfillment_key] = uniqueFullfillment_ids

        if filters.get(fulfillment_key):
            fulfillment_ids = list(map(str.strip, filters[fulfillment_key].split(",")))
            original_fulfillment_ids.update(fulfillment_ids)
            for ffid_chunk in chunk_list(fulfillment_ids, 10):
                # fetch fullfillment details from SALESFULLFILLMENT
                graphql_request.append({
                    "url": path['SALESFULLFILLMENT'],
                    "query": fetch_Fullfillment_query(ffid_chunk)
                })
                # also fetch KeySphere mapping from FID
                graphql_request.append({
                    "url": path['FID'],
                    "query": fetch_keysphereFullfillment_query(ffid_chunk)
                })

    if "Sales_Order_id" in filters:
        salesOrder_key = "Sales_Order_id"
        uniqueSalesOrder_ids = ",".join(sorted(set(filters[salesOrder_key].split(','))))
        filters[salesOrder_key] = uniqueSalesOrder_ids

        if filters.get(salesOrder_key):
            salesorder_ids = list(map(str.strip, filters[salesOrder_key].split(",")))
            original_salesorder_ids.update(salesorder_ids)
            for so_chunk in chunk_list(salesorder_ids, 10):
                graphql_request.append({
                    "url": path['SALESFULLFILLMENT'],
                    "query": fetch_salesOrder_query(so_chunk)
                })
                graphql_request.append({
                    "url": path['FID'],
                    "query": fetch_keysphereSalesorder_query(so_chunk)
                })

    if "wo_id" in filters:
        workOrder_key = "wo_id"
        uniqueWorkOrder_ids = ",".join(sorted(set(filters[workOrder_key].split(','))))
        filters[workOrder_key] = uniqueWorkOrder_ids

        if filters.get(workOrder_key):
            workorder_ids = list(map(str.strip, filters[workOrder_key].split(",")))
            original_workorder_ids.update(workorder_ids)
            for wo_chunk in chunk_list(workorder_ids, 10):
                # fetch workorder details
                graphql_request.append({
                    "url": path['WORKORDER'],
                    "query": fetch_workOrder_query(wo_chunk)
                })
                # fetch KeySphere mapping from FID for workorder -> sales/fulfillment relations
                graphql_request.append({
                    "url": path['FID'],
                    "query': fetch_keysphereWorkorder_query(wo_chunk)
                })

    # run the initial set of requests
    results = asyncio.run(run_all(graphql_request)) if graphql_request else []

    # parse FID results to collect related salesorder_ids & fulfillment_ids
    related_salesorders = set()
    related_fulfillments = set()

    for res in results:
        if not isinstance(res, dict):
            continue
        data = res.get("data", {}) or {}
        # KeySphere responses - detect the getBy* keys
        if "getByWorkorderids" in data:
            items = safe_get(data, ["getByWorkorderids", "result"], [])
            for it in listify(items):
                so = it.get("salesOrder") or {}
                if so and so.get("salesOrderId"):
                    related_salesorders.add(str(so.get("salesOrderId")))
                # sometimes there may be other fields for fulfillments; check asnNumbers or others
        if "getBySalesorderids" in data:
            items = safe_get(data, ["getBySalesorderids", "result"], [])
            for it in listify(items):
                so = it.get("salesOrder") or {}
                if so and so.get("salesOrderId"):
                    related_salesorders.add(str(so.get("salesOrderId")))
        if "getByFulfillmentids" in data:
            items = safe_get(data, ["getByFulfillmentids", "result"], [])
            for it in listify(items):
                so = it.get("salesOrder") or {}
                if so and so.get("salesOrderId"):
                    related_salesorders.add(str(so.get("salesOrderId")))
                # Note: KeySphere result may contain other identifiers; if there were fulfillment ids returned, you could capture them too

    # remove those already requested directly
    related_salesorders = related_salesorders - original_salesorder_ids
    related_fulfillments = related_fulfillments - original_fulfillment_ids

    # If KeySphere gave us related SalesOrders or Fulfillments, fetch full details from SALESFULLFILLMENT
    extra_requests = []
    if related_salesorders:
        related_list = sorted(list(related_salesorders))
        for so_chunk in chunk_list(related_list, 10):
            extra_requests.append({
                "url": path['SALESFULLFILLMENT'],
                "query": fetch_salesOrder_query(so_chunk)
            })
            # also optionally get KeySphere mapping for those sales orders (not required, but kept symmetrical)
            extra_requests.append({
                "url": path['FID'],
                "query": fetch_keysphereSalesorder_query(so_chunk)
            })

    if related_fulfillments:
        related_list = sorted(list(related_fulfillments))
        for ff_chunk in chunk_list(related_list, 10):
            extra_requests.append({
                "url": path['SALESFULLFILLMENT'],
                "query": fetch_Fullfillment_query(ff_chunk)
            })
            extra_requests.append({
                "url": path['FID'],
                "query": fetch_keysphereFullfillment_query(ff_chunk)
            })

    if extra_requests:
        extra_results = asyncio.run(run_all(extra_requests))
        # append extra results to original results so newOutputFormat can see all SALESFULLFILLMENT responses
        results.extend(list(extra_results))

    return results

# ---------- Output Format (full function taken from earlier provided code) ----------
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
            if not isinstance(item, dict):
                continue
            data = item.get("data") or {}
            
            if not data:
                continue

            soids_data = data.get("getSalesOrderBySoids")
            ffids_data = data.get("getSalesOrderByFfids")
            
            sales_orders = extract_sales_order(data)
            if not sales_orders or len(sales_orders) == 0:
                continue

            for so in listify(sales_orders):

                fulfillments = safe_get(so, ['fulfillments'])

                if isinstance(fulfillments, dict):
                    fulfillments = [fulfillments]

                if filtersValue:
                    if soids_data and not ffids_data:
                        sales_order_id = safe_get(so, ['salesOrderId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(sales_order_id)
                    elif ffids_data and not soids_data:
                        fulfillment_id = safe_get(fulfillments, [0, 'fulfillmentId'])
                        if region and region.upper() == safe_get(so, ['region'], "").upper():
                            ValidCount.append(fulfillment_id)
                
                if region and region.upper() != safe_get(so, ['region'], "").upper():
                    continue

                shipping_addr = pick_address_by_type(so, "SHIPPING")
                shipping_phone = pick_address_by_type(fulfillments[0] if fulfillments else {}, "SHIPPING")
                billing_addr = pick_address_by_type(so, "BILLING")
                shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""
                              
                lob_list = list(filter(
                            lambda lob: lob is not None and str(lob).strip() != "",
                            map(
                                lambda line: safe_get(line, ['lob']),
                                safe_get(fulfillments, [0,'salesOrderLines']) or []
                            )
                        ))
                
                lob = ", ".join(lob_list)

                facility_list = list(filter(
                            lambda facility: facility is not None and str(facility).strip() != "",
                            map(
                                lambda line: safe_get(line, ['facility']),
                                safe_get(fulfillments, [0,'salesOrderLines']) or []
                            )
                        ))

                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f and str(f).strip()))

                def get_status_date(code):
                    status_code = safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(fulfillments, [0, 'soStatus', 0, 'statusDate']))
                    return ""
                
                row = {
                    "Fulfillment ID": safe_get(fulfillments, [0, 'fulfillmentId']),
                    "BUID": safe_get(so, ['buid']),
                    "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                    "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "LOB": lob,
                    "Sales Order ID": safe_get(so, ['salesOrderId']),
                    "Agreement ID": safe_get(so, ['agreementId']),
                    "Amount": safe_get(so, ['totalPrice']),
                    "Currency Code": safe_get(so, ['currency']),
                    "Customer Po Number": safe_get(so, ['poNumber']),
                    "Delivery City": safe_get(fulfillments, [0, 'deliveryCity']),
                    "DOMS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "Dp ID": safe_get(so, ['dpid']),
                    "Fulfillment Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                    "Merge Type": safe_get(fulfillments, [0, 'mergeType']),
                    "InstallInstruction2": get_install_instruction2_id(so),
                    "PP Date": get_status_date("PP"),
                    "IP Date": get_status_date("IP"),
                    "MN Date": get_status_date("MN"),
                    "SC Date": get_status_date("SC"),
                    "Location Number": safe_get(so, ['locationNum']),
                    "OFS Status Code": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "OFS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                    "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "ShippingContactName": shipping_contact_name,
                    "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                    "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                    "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                    "ShipToPhone": (listify(shipping_phone.get("phone", []))[0].get("phoneNumber", "")
                                    if shipping_phone and listify(shipping_phone.get("phone", [])) else ""),
                    "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                    "Order Age": safe_get(so, ['orderDate']),
                    "Order Amount usd": safe_get(so, ['rateUsdTransactional']),
                    "Rate Usd Transactional": safe_get(so, ['rateUsdTransactional']),
                    "Sales Rep Name": safe_get(so, ['salesrep', 0, 'salesRepName']),
                    "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Source System Status": safe_get(fulfillments, [0, 'soStatus', 0,'sourceSystemStsCode']),
                    "Tie Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'soLineNum']),
                    "Si Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'siNumber']),
                    "Req Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                    "Reassigned IP Date": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                    "Payment Term Code": safe_get(fulfillments, [0, 'paymentTerm']),
                    "Region Code": safe_get(so, ['region']),
                    "FO ID": safe_get(fulfillments, [0, 'fulfillmentOrder', 0, 'foId']),
                    "System Qty": safe_get(fulfillments, [0, 'systemQty']),
                    "Ship By Date": safe_get(fulfillments, [0, 'shipByDate']),
                    "Facility": facility,
                    "Tax Regstrn Num": safe_get(fulfillments, [0, 'address', 0, 'taxRegstrnNum']),
                    "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
                    "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                    "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
                    "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
                    "Country": shipping_addr.get("country", "") if shipping_addr else "",
                    "Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                    "Must Arrive By Date": dateFormation(safe_get(fulfillments, [0, 'mustArriveByDate'])),
                    "Manifest Date": dateFormation(safe_get(fulfillments, [0, 'manifestDate'])),
                    "Revised Delivery Date": dateFormation(safe_get(fulfillments, [0, 'revisedDeliveryDate'])),
                    "Source System ID": safe_get(so, ['sourceSystemId']),
                    "OIC ID": safe_get(fulfillments, [0, 'oicId']),
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
                # If tablestructural exists, use it; otherwise return rows as simple object
                table_grid_output = tablestructural(rows,region) if 'tablestructural' in globals() and callable(globals().get('tablestructural')) else {"rows": rows, "region": region}
                
                if filtersValue:
                    table_grid_output["Count"] = count_valid
                ValidCount.clear()
                return table_grid_output

    except Exception as e:
        return {"error": str(e)}

# ---------- Example runner ----------
if __name__ == "__main__":
    # Example usage:
    # Provide filters (choose one of Sales_Order_id, Fullfillment Id, or wo_id)
    # You can replace these with your real IDs.
    example_filters = {
        # "Sales_Order_id": "SO123,SO456",
        # "Fullfillment Id": "FF123,FF456",
        "wo_id": "WO123,WO456"
    }
    region = "APJ"          # EMEA | APJ | DAO/AMER/LA
    format_type = "grid"    # or "export" or None

    # Fetch raw GraphQL responses (this will call WORKORDER / SALESFULLFILLMENT and KeySphere FID mapping)
    raw = newmainfunction(example_filters, format_type, region)

    # Format them into structured output using newOutputFormat
    formatted = newOutputFormat(raw, format_type=format_type, region=region, filtersValue=True)

    print(json.dumps(formatted, indent=2))
