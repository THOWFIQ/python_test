@app.route('/getByFilters', methods=['POST'])
def get_by_filters():
    try:
        # Extract incoming JSON
        data = request.get_json()
        filters = data.get("filters", {})
        tableformat = data.get("tableformat")
        region = data.get("region")

        # Validate input
        if not filters or not tableformat or not region:
            return jsonify({"error": "Please provide 'filters', 'tableformat' (grid or export), and 'region'."}), 400

        # Reset global CollectedValue tracker
        global CollectedValue
        CollectedValue = {
            "sales": False,
            "FullFil": False,
            "Work": False,
            "Fo": False
        }

        # Fetch combined results using filters
        result_map = fieldValidation(filters=filters, format_type=tableformat, region=region)

        # Format output based on requested format
        formatted_output = OutputFormat(result_map, format_type=tableformat)

        return jsonify(formatted_output), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
