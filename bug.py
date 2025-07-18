import json
import os
import sys
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

# Fix the path so Python can find local files
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import your GraphQL query functions from queries.py
from queries import (
    fetch_soaorder_query,
    fetch_salesorder_query,
    fetch_workOrderId_query,
    fetch_getByWorkorderids_query,
    fetch_fulfillment_query,
    fetch_getFulfillmentsBysofulfillmentid_query,
    fetch_getAllFulfillmentHeadersSoidFulfillmentid_query,
    fetch_getFbomBySoFulfillmentid_query,
    fetch_foid_query,
    tablestructural
)
