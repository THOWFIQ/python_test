import os import json import httpx from concurrent.futures import ThreadPoolExecutor from graphqlQueries import ( fetch_salesorder_query, fetch_fulfillment_query, fetch_workorder_query, fetch_bom_query, tablestructural, get_path )

Load config file

configABSpath = os.path.abspath(os.path.join(os.path.dirname(file), '..', 'config', 'config_ge4.json')) with open(configABSpath, 'r') as file: configPath = json.load(file)

region = "DAO"  # Can be parameterized as needed

FID = get_path(region, 'Linkage', configPath) FOID = get_path(region, 'FM_Order', configPath) SOPATH = get_path(region, 'SO_Header', configPath) NOID = get_path(region, 'WO_Details', configPath) FFBOM = get_path(region, 'FM_BOM', configPath)

def getbySalesOrderID(salesorder_ids): def fetch_data(salesorder_id): variables = {"salesorderIds": [salesorder_id]} response_so = httpx.post(SOPATH, json={"query": fetch_salesorder_query(), "variables": variables}) response_fo = httpx.post(FOID, json={"query": fetch_fulfillment_query(), "variables": variables}) response_no = httpx.post(NOID, json={"query": fetch_workorder_query(), "variables": variables}) response_fb = httpx.post(FFBOM, json={"query": fetch_bom_query(), "variables": variables})

if response_so.status_code == 200 and response_fo.status_code == 200 \
        and response_no.status_code == 200 and response_fb.status_code == 200:
        return {
            "salesorder": response_so.json(),
            "fulfillment": response_fo.json(),
            "workorder": response_no.json(),
            "bom": response_fb.json(),
        }
    else:
        return {"error": f"Failed to fetch data for {salesorder_id}"}

results = []
with ThreadPoolExecutor() as executor:
    future_to_id = {executor.submit(fetch_data, sid): sid for sid in salesorder_ids}
    for future in future_to_id:
        result = future.result()
        results.append(result)

return results

def get_flattened_data(raw_data): flattened = [] for entry in raw_data: if "error" in entry: continue

so_data = entry.get("salesorder", {}).get("data", {})
    fo_data = entry.get("fulfillment", {}).get("data", {})
    no_data = entry.get("workorder", {}).get("data", {})
    fb_data = entry.get("bom", {}).get("data", {})

    # Flatten logic here: merge fields into a single dict
    combined = {
        "so": so_data,
        "fo": fo_data,
        "no": no_data,
        "fb": fb_data
    }
    flattened.append(combined)

return flattened

def get_columns_and_data(salesorder_ids): raw_data = getbySalesOrderID(salesorder_ids) flattened_data = get_flattened_data(raw_data) columns = tablestructural() return { "columns": columns, "data": flattened_data }

