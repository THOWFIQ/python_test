Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 267, in <module>
    result = fileldValidation(filters, format_type='grid', region="EMEA")
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 247, in fileldValidation
    result_map["Fullfillment Id"] = [f.result() for f in as_completed([executor.submit(combined_fulfillment_fetch, x) for x in fids])]
                                     ^^^^^^^^^^
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\_base.py", line 401, in __get_result
    raise self._exception
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\thread.py", line 59, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 110, in combined_fulfillment_fetch
    combined['data']['getFbomBySoFulfillmentid'] = post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id), vars).get("data", {}).get("getFbomBySoFulfillmentid", [])
                                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'NoneType' object has no attribute 'get'
