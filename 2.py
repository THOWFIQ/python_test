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

def chunk_list(data_list, chunk_size):
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]

# Rest of your code remains unchanged
