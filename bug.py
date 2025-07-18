Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\salesOrderWrapper.py", line 206, in <module>
    getbySalesOrderID(salesorderid=salesorderIds,format_type=format_type,region=region)
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\salesOrderWrapper.py", line 186, in getbySalesOrderID
    result = future.result()
             ^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\_base.py", line 401, in __get_result
    raise self._exception
  File "C:\Users\Thowfiq_S\AppData\Local\Programs\Python\Python312\Lib\concurrent\futures\thread.py", line 59, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\salesOrderWrapper.py", line 65, in getbySalesOrderIDs
    result = salesorder["data"]["getBySalesorderids"]["result"][0]
             ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^
TypeError: 'NoneType' object is not subscriptable
