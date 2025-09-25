def getPath(region):
    try:
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
