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

Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\app.py", line 30, in <module>
    from OrderLookUpFunction.salesOrderWrapper import getbySalesOrderID
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\salesOrderWrapper.py", line 34, in <module>
    from graphqlQueries import *
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\graphqlQueries.py", line 2, in <module>
    configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
                    ^^
NameError: name 'os' is not defined. Did you forget to import 'os'?
