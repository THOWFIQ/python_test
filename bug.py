if __name__ == "__main__":
    salesorderIds=["1004452326"]
    region = "EMEA"
    # salesorderIds=["1004452326", "1004543337"]
    # cleaned = fetch_and_clean()
    # print(cleaned)
    format_type='grid' #grid/export
    getbySalesOrderID(salesorderid=salesorderIds,format_type=format_type,region=region)
