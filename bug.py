try:
        data = request.get_json()
        salesorderids = data.get("salesorderids")
        format_type = data.get("format_type", "grid")
        region = data.get("region")

        if not salesorderids or not isinstance(salesorderids, list):
            return jsonify({"error": "salesorderids must be a non-empty list"}), 400
        if format_type not in ["grid", "export"]:
            return jsonify({"error": "format_type must be 'grid' or 'export'"}), 400
        if not region:
            return jsonify({"error": "region is required"}), 400

        response = getbySalesOrderID(salesorderids, format_type, region)
        return jsonify(json.loads(response))  # already JSON dumped in function
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
