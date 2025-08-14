for i in work_orders:
        workOrderId = i["woId"]

        workOrderId_query = fetch_workOrderId_query(workOrderId)

        getWorkOrderById=post_api(URL=WOID, query=workOrderId_query, variables=None)
       
        getByWorkorderids_query = fetch_getByWorkorderids_query(workOrderId)

        sn_numbers = []
        getByWorkorderids=post_api(URL=FID, query=getByWorkorderids_query, variables=None)
        if getByWorkorderids and getByWorkorderids.get('data') is not None:
          sn_numbers = [
              sn.get("snNumber") 
              for sn in getByWorkorderids["data"]["getByWorkorderids"]["result"][0]["asnNumbers"]
              if sn.get("snNumber") is not None
          ]
        
        if getWorkOrderById and getWorkOrderById.get('data') is not None:
            wo_detail = getWorkOrderById["data"]["getWorkOrderById"][0]
            flattened_wo = {
                "Vendor Work Order Num": wo_detail["woId"],
                "Channel Status Code": wo_detail["channelStatusCode"],
                "Ismultipack": wo_detail["woLines"][0].get("ismultipack"),  # just the first line's value
                "Ship Mode": wo_detail["shipMode"],
                "Is Otm Enabled": wo_detail["isOtmEnabled"],
                "SN Number": sn_numbers
            }
            
            for i, wo in enumerate(work_orders):
                if wo.get("woId") == wo_detail["woId"]:
                    work_orders[i] = flattened_wo.copy()

    # print(f"work order data : {work_orders}")

    # result_list = salesorder.get("data", {}).get("getBySalesorderids", {}).get("result", [])
    wo_ids = [wo for wo in work_orders]
