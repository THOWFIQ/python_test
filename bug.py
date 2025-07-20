def combined_fulfillment_fetch(fulfillment_id):
    combined = {'data': {}}
    vars = {"fulfillment_id": fulfillment_id}

    try:
        response = post_api(SOPATH, fetch_fulfillment_query(), vars)
        combined['data']['getFulfillmentsById'] = response.get("data", {}).get("getFulfillmentsById", []) if response else []
    except Exception as e:
        print(f"[ERROR] getFulfillmentsById failed: {e}")
        combined['data']['getFulfillmentsById'] = []

    try:
        response = post_api(SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id), vars)
        combined['data']['getFulfillmentsBysofulfillmentid'] = response.get("data", {}).get("getFulfillmentsBysofulfillmentid", []) if response else []
    except Exception as e:
        print(f"[ERROR] getFulfillmentsBysofulfillmentid failed: {e}")
        combined['data']['getFulfillmentsBysofulfillmentid'] = []

    try:
        response = post_api(FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id), vars)
        combined['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = response.get("data", {}).get("getAllFulfillmentHeadersSoidFulfillmentid", []) if response else []
    except Exception as e:
        print(f"[ERROR] getAllFulfillmentHeadersSoidFulfillmentid failed: {e}")
        combined['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = []

    try:
        response = post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id), vars)
        combined['data']['getFbomBySoFulfillmentid'] = response.get("data", {}).get("getFbomBySoFulfillmentid", []) if response else []
    except Exception as e:
        print(f"[ERROR] getFbomBySoFulfillmentid failed: {e}")
        combined['data']['getFbomBySoFulfillmentid'] = []

    return combined
