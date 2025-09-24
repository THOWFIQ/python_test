import os
import sys
import json
import traceback
import nest_asyncio
import asyncio
import aiohttp
from collections import defaultdict
from typing import List, Dict, Optional

nest_asyncio.apply()

# ensure local module path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# your query builders (from the file you already gave me)
from graphqlQueries_new import (
    fetch_salesOrder_query,
    fetch_Fullfillment_query,
    fetch_workOrder_query,
    fetch_keysphereSalesorder_query,
    fetch_keysphereFullfillment_query,
    fetch_keysphereWorkorder_query,
)

# load config (same as you had)
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

# ----------------- helpers -----------------
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

# ----------------- GraphQL async helpers -----------------
async def fetch_graphql(session, url, query):
    async with session.post(url, json={"query": query}) as response:
        try:
            return await response.json()
        except Exception:
            text = await response.text()
            try:
                return json.loads(text)
            except Exception:
                return {"error": "invalid_json_response", "text": text}

async def run_all(requests_meta: List[Dict]):
    """
    requests_meta: list of dicts {url, query, type, ids}
    returns list of responses in the same order
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_graphql(session, req["url"], req["query"]) for req in requests_meta]
        results = await asyncio.gather(*tasks)
        return results

# ----------------- Main orchestration -----------------
def newmainfunction(filters, format_type, region):
    """
    Orchestrates:
      - fetch SALESFULLFILLMENT (SO / FF) or WORKORDER (WO)
      - fetch KeySphere (FID) mapping for same IDs -> get related woIds
      - fetch WORKORDER by the derived woIds
      - attach workorder data to the salesOrder objects
      - return combined result_map (modified SALESFULLFILLMENT responses) to be fed to newOutputFormat
    """
    region = (region or "").upper()
    path = getPath(region)
    requests_meta = []

    original_salesorder_ids = set()
    original_fulfillment_ids = set()
    original_workorder_ids = set()

    # prepare initial requests
    if "Sales_Order_id" in filters:
        key = "Sales_Order_id"
        unique_ids = ",".join(sorted(set(filters[key].split(','))))
        filters[key] = unique_ids
        if filters.get(key):
            ids = list(map(str.strip, filters[key].split(",")))
            original_salesorder_ids.update(ids)
            for chunk in chunk_list(ids, 10):
                requests_meta.append({
                    "url": path['SALESFULLFILLMENT'],
                    "query": fetch_salesOrder_query(chunk),
                    "type": "SO_SF",
                    "ids": chunk
                })
                # KeySphere mapping for these salesorders -> might return workOrders
                requests_meta.append({
                    "url": path['FID'],
                    "query": fetch_keysphereSalesorder_query(chunk),
                    "type": "SO_FID",
                    "ids": chunk
                })

    if "Fullfillment Id" in filters:
        key = "Fullfillment Id"
        unique_ids = ",".join(sorted(set(filters[key].split(','))))
        filters[key] = unique_ids
        if filters.get(key):
            ids = list(map(str.strip, filters[key].split(",")))
            original_fulfillment_ids.update(ids)
            for chunk in chunk_list(ids, 10):
                requests_meta.append({
                    "url": path['SALESFULLFILLMENT'],
                    "query": fetch_Fullfillment_query(chunk),
                    "type": "FF_SF",
                    "ids": chunk
                })
                requests_meta.append({
                    "url": path['FID'],
                    "query': fetch_keysphereFullfillment_query(chunk),
                    "type": "FF_FID",
                    "ids": chunk
                })

    if "wo_id" in filters:
        key = "wo_id"
        unique_ids = ",".join(sorted(set(filters[key].split(','))))
        filters[key] = unique_ids
        if filters.get(key):
            ids = list(map(str.strip, filters[key].split(",")))
            original_workorder_ids.update(ids)
            for chunk in chunk_list(ids, 10):
                # fetch workorder details direct
                requests_meta.append({
                    "url": path['WORKORDER'],
                    "query": fetch_workOrder_query(chunk),
                    "type": "WO_WO",
                    "ids": chunk
                })
                # use KeySphere FID for the same wo -> it may map back to sales orders
                requests_meta.append({
                    "url": path['FID'],
                    "query": fetch_keysphereWorkorder_query(chunk),
                    "type": "WO_FID",
                    "ids": chunk
                })

    if not requests_meta:
        return []

    # execute initial requests
    initial_responses = asyncio.run(run_all(requests_meta))

    # build mappings
    sales_to_woids = defaultdict(set)   # salesOrderId -> set of woIds
    woids_set = set()
    soid_to_so_objs = defaultdict(list)  # salesOrderId -> list of references to SO dicts (we will mutate these)
    modified_sales_sf_responses = []     # store the responses that are SALESFULLFILLMENT so we can return them later

    # process initial responses to gather mappings & also collect salesfullfillment SO objects
    for req, resp in zip(requests_meta, initial_responses):
        rtype = req.get("type")
        data = (resp or {}).get("data", {}) if isinstance(resp, dict) else {}
        # parse KeySphere responses
        if rtype == "SO_FID":
            items = safe_get(data, ["getBySalesorderids", "result"], [])
            for it in listify(items):
                so = it.get("salesOrder") or {}
                sales_id = safe_get(so, ['salesOrderId'])
                workOrders = listify(it.get("workOrders", []))
                # workOrders elements may be dicts with 'woId' or primitive; handle both
                for w in workOrders:
                    if isinstance(w, dict):
                        woid = w.get("woId")
                    else:
                        woid = w
                    if woid:
                        sales_to_woids[str(sales_id)].add(str(woid))
                        woids_set.add(str(woid))

        elif rtype == "FF_FID":
            items = safe_get(data, ["getByFulfillmentids", "result"], [])
            for it in listify(items):
                so = it.get("salesOrder") or {}
                sales_id = safe_get(so, ['salesOrderId'])
                workOrders = listify(it.get("workOrders", []))
                for w in workOrders:
                    if isinstance(w, dict):
                        woid = w.get("woId")
                    else:
                        woid = w
                    if woid:
                        sales_to_woids[str(sales_id)].add(str(woid))
                        woids_set.add(str(woid))

        elif rtype == "WO_FID":
            items = safe_get(data, ["getByWorkorderids", "result"], [])
            for it in listify(items):
                so = it.get("salesOrder") or {}
                sales_id = safe_get(so, ['salesOrderId'])
                workOrders = listify(it.get("workOrders", []))
                # if mapping directly gives woId, map sales -> wo
                for w in workOrders:
                    if isinstance(w, dict):
                        woid = w.get("woId")
                    else:
                        woid = w
                    if woid:
                        if sales_id:
                            sales_to_woids[str(sales_id)].add(str(woid))
                        woids_set.add(str(woid))

        # collect salesfullfillment responses (so we can later attach workOrders)
        elif rtype in ("SO_SF", "FF_SF"):
            # keep the response reference so we can mutate it
            if isinstance(resp, dict):
                modified_sales_sf_responses.append(resp)
                # if response contains salesOrders we register references
                so_list = []
                if "getSalesOrderBySoids" in (resp.get("data") or {}):
                    so_list = safe_get(resp, ["data", "getSalesOrderBySoids", "salesOrders"], [])
                elif "getSalesOrderByFfids" in (resp.get("data") or {}):
                    so_list = safe_get(resp, ["data", "getSalesOrderByFfids", "salesOrders"], [])
                for so in listify(so_list):
                    so_id = safe_get(so, ['salesOrderId'])
                    if so_id:
                        soid_to_so_objs[str(so_id)].append(so)

        # collect direct workorder responses (if input included wo_id and we fetched WORKORDER directly), store later attachment
        # We'll process WO_WO responses in the next step below.

    # If initial requests included WORKORDER responses (WO_WO), we want to capture them too:
    # collect direct WORKORDER responses (if present) so we can attach them back to sales objects (via mapping from FID)
    direct_workorder_data = {}  # woId -> workorder object
    for req, resp in zip(requests_meta, initial_responses):
        if req.get("type") == "WO_WO" and isinstance(resp, dict):
            # fetch result from response
            payload = resp.get("data") or {}
            # The workorder query's root might be 'getWOrkOrderByWoIds' (as in your query file).
            work_list = None
            if "getWOrkOrderByWoIds" in payload:
                work_list = safe_get(payload, ["getWOrkOrderByWoIds"])
            elif "getWorkOrderByWoIds" in payload:
                work_list = safe_get(payload, ["getWorkOrderByWoIds"])
            else:
                work_list = payload  # fallback
            for w in listify(work_list):
                woid = safe_get(w, ['woId']) or safe_get(w, ['woId'])
                if woid:
                    direct_workorder_data[str(woid)] = w

    # Add explicitly discovered woIds from KeySphere (woids_set) to the list of workorder ids to fetch
    # If we already have direct_workorder_data for some woids, we won't re-fetch them.
    to_fetch_woids = sorted([w for w in woids_set if w not in direct_workorder_data])

    # fetch missing workorder details if any
    fetched_workorder_data = {}  # woId -> workorder object
    if to_fetch_woids:
        work_requests = []
        for chunk in chunk_list(to_fetch_woids, 10):
            work_requests.append({
                "url": path['WORKORDER'],
                "query": fetch_workOrder_query(chunk),
                "type": "WO_WO_FETCH",
                "ids": chunk
            })
        work_responses = asyncio.run(run_all(work_requests))
        for resp in work_responses:
            if not isinstance(resp, dict):
                continue
            payload = resp.get("data") or {}
            if "getWOrkOrderByWoIds" in payload:
                work_list = safe_get(payload, ["getWOrkOrderByWoIds"])
            elif "getWorkOrderByWoIds" in payload:
                work_list = safe_get(payload, ["getWorkOrderByWoIds"])
            else:
                work_list = payload
            for w in listify(work_list):
                woid = safe_get(w, ['woId'])
                if woid:
                    fetched_workorder_data[str(woid)] = w

    # combine direct + fetched workorder data
    woid_to_workdata = {}
    woid_to_workdata.update(direct_workorder_data)
    woid_to_workdata.update(fetched_workorder_data)

    # Attach workOrders to sales order objects we have (soid_to_so_objs)
    for sales_id, so_objs in soid_to_so_objs.items():
        related_woids = sorted(list(sales_to_woids.get(str(sales_id), [])))
        attached = []
        for woid in related_woids:
            if woid in woid_to_workdata:
                attached.append(woid_to_workdata[woid])
            else:
                # attach stub if we don't have full data
                attached.append({"woId": woid})
        for so in so_objs:
            # place the related workOrders under key 'workOrders' (new key)
            so['workOrders'] = attached

    # Also: If we had only WO input and KeySphere returned related salesOrders that we didn't fetch earlier,
    # fetch those SALESFULLFILLMENT details now so output format has complete salesOrders
    # Determine missing sales orders that need fetching
    # from earlier mapping sales_to_woids we may be able to invert mapping to discover sales ids if any were discovered via WO_FID
    discovered_sales_ids = set(sales_to_woids.keys()) - original_salesorder_ids
    extra_so_requests = []
    if discovered_sales_ids:
        for chunk in chunk_list(sorted(list(discovered_sales_ids)), 10):
            extra_so_requests.append({
                "url": path['SALESFULLFILLMENT'],
                "query": fetch_salesOrder_query(chunk),
                "type": "SO_SF_EXTRA",
                "ids": chunk
            })
            # fetch KeySphere for symmetry (optional)
            extra_so_requests.append({
                "url": path['FID'],
                "query": fetch_keysphereSalesorder_query(chunk),
                "type": "SO_FID_EXTRA",
                "ids": chunk
            })
    if extra_so_requests:
        extra_resps = asyncio.run(run_all(extra_so_requests))
        # add these responses to modified_sales_sf_responses and register the so objects so they get workOrders attached
        for resp in extra_resps:
            if not isinstance(resp, dict):
                continue
            modified_sales_sf_responses.append(resp)
            payload = resp.get("data") or {}
            so_list = []
            if "getSalesOrderBySoids" in payload:
                so_list = safe_get(resp, ["data", "getSalesOrderBySoids", "salesOrders"], [])
            elif "getSalesOrderByFfids" in payload:
                so_list = safe_get(resp, ["data", "getSalesOrderByFfids", "salesOrders"], [])
            for so in listify(so_list):
                so_id = safe_get(so, ['salesOrderId'])
                if so_id:
                    soid_to_so_objs[str(so_id)].append(so)
        # after appending, re-attach workOrders to newly fetched SOs
        for sales_id, so_objs in soid_to_so_objs.items():
            related_woids = sorted(list(sales_to_woids.get(str(sales_id), [])))
            attached = []
            for woid in related_woids:
                if woid in woid_to_workdata:
                    attached.append(woid_to_workdata[woid])
                else:
                    attached.append({"woId": woid})
            for so in so_objs:
                so['workOrders'] = attached

    # prepare final result_map to send to newOutputFormat:
    # include only the SALESFULLFILLMENT response objects (modified_sales_sf_responses)
    final_result_map = []
    # we kept modified_sales_sf_responses from initial responses and appended extras
    for resp in modified_sales_sf_responses:
        final_result_map.append(resp)

    return final_result_map

# ----------------- Output Format (extended to include WorkOrder fields) -----------------
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

            sales_orders = extract_sales_order(data)
            if not sales_orders or len(sales_orders) == 0:
                continue

            for so in listify(sales_orders):
                fulfillments = safe_get(so, ['fulfillments'])
                if isinstance(fulfillments, dict):
                    fulfillments = [fulfillments]

                # attach count tracking when filtersValue provided (keeps previous behavior)
                if filtersValue:
                    soids_data = data.get("getSalesOrderBySoids")
                    ffids_data = data.get("getSalesOrderByFfids")
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
                    map(lambda line: safe_get(line, ['lob']), safe_get(fulfillments, [0, 'salesOrderLines']) or [])
                ))
                lob = ", ".join(lob_list)

                facility_list = list(filter(
                    lambda facility: facility is not None and str(facility).strip() != "",
                    map(lambda line: safe_get(line, ['facility']), safe_get(fulfillments, [0, 'salesOrderLines']) or [])
                ))
                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f and str(f).strip()))

                def get_status_date(code):
                    status_code = safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(fulfillments, [0, 'soStatus', 0, 'statusDate']))
                    return ""

                # --- Extract workorder related fields if present ---
                workorders = listify(so.get("workOrders", []))
                wo_ids = [str(safe_get(w, ['woId']) or safe_get(w, ['woId']) or "") for w in workorders if safe_get(w, ['woId']) or safe_get(w, ['woId'])]
                wo_channel_vals = list(dict.fromkeys([str(safe_get(w, ['channel']) or "") for w in workorders if safe_get(w, ['channel'])]))
                wo_type_vals = list(dict.fromkeys([str(safe_get(w, ['woType']) or "") for w in workorders if safe_get(w, ['woType'])]))
                wo_shipmode_vals = list(dict.fromkeys([str(safe_get(w, ['shipMode']) or "") for w in workorders if safe_get(w, ['shipMode'])]))
                wo_vendor_vals = list(dict.fromkeys([str(safe_get(w, ['vendorSiteId']) or "") for w in workorders if safe_get(w, ['vendorSiteId'])]))

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
                    "Order Type": dateFormation(safe_get(so, ['orderType'])),

                    # --- appended WorkOrder fields ---
                    "WorkOrder IDs": ", ".join([w for w in wo_ids if w]),
                    "WO Channel": ", ".join([v for v in wo_channel_vals if v]),
                    "WO Type": ", ".join([v for v in wo_type_vals if v]),
                    "WO ShipMode": ", ".join([v for v in wo_shipmode_vals if v]),
                    "WO VendorSiteId": ", ".join([v for v in wo_vendor_vals if v])
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
                    "Must Arrive By Date","Manifest Date","Revised Delivery Date","Source System ID","OIC ID","Order Date","Order Type",
                    # workorder fields should be placed at end (or wherever you prefer)
                    "WorkOrder IDs","WO Channel","WO Type","WO ShipMode","WO VendorSiteId"
                ]

                rows = []
                for item in flat_list:
                    reordered_values = [item.get(key, "") for key in desired_order]
                    row = {"columns": [{"value": val if val is not None else ""} for val in reordered_values]}
                    rows.append(row)
                table_grid_output = tablestructural(rows,region) if 'tablestructural' in globals() and callable(globals().get('tablestructural')) else {"rows": rows, "region": region}

                if filtersValue:
                    table_grid_output["Count"] = count_valid
                ValidCount.clear()
                return table_grid_output

    except Exception as e:
        return {"error": str(e)}

# ----------------- Example runner -----------------
if __name__ == "__main__":
    # Example: pass Sales Order ids, will fetch SALESFULLFILLMENT, FID -> derive woIds -> fetch WORKORDER -> attach -> output
    example_filters = {
        "Sales_Order_id": "SO123,SO456"
        # or "Fullfillment Id": "FF123,FF456"
        # or "wo_id": "WO123,WO456"
    }
    region = "APJ"
    format_type = "grid"

    raw = newmainfunction(example_filters, format_type, region)
    formatted = newOutputFormat(raw, format_type=format_type, region=region, filtersValue=True)

    print(json.dumps(formatted, indent=2))
