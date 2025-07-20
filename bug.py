Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 226, in <module>
    result = fileldValidation(filters=filters, format_type='grid', region='EMEA')
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 203, in fileldValidation
    result_map['wo_id'] = [f.result() for f in as_completed(futures)]
                           ^^^^^^^^^^
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\_base.py", line 401, in __get_result
    raise self._exception
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\thread.py", line 59, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 59, in combined_woid_fetch
    wo_detail = wo_data.get('data', {}).get('getWorkOrderById', [{}])[0]
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'NoneType' object has no attribute 'get'
