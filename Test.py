@app.route('/getbySalesOrderId', methods=['POST'])
def getbySalesbyOrderId():
    # Get query parameters
    data = request.get_json()
    salesorderid = data.get('salesorderid', "")
    tableformat = data.get('tableformat')
    region = data.get('region')

    try:
        if salesorderid and tableformat:
            salesorder_ids = salesorderid.split(",") if salesorderid else []
            salesorderdetails = getbySalesOrderID(salesorderid=salesorder_ids,format_type=tableformat,region=region)
            salesorderdetails = json.loads(salesorderdetails)
            return jsonify(salesorderdetails), 200
        else:
            return jsonify({"error": "Error: Please Provide Inputs salesorderid and tableformat (grid or export)"}), 400
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Failed to connect to the GraphQL endpoint. Error: {e}"}), 500
