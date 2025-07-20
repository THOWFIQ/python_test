@app.route('/GetTemplates', methods=['GET'])
def get_templates():
    if getattr(g, 'skip_logs', False):
        return jsonify({'message': 'Logs skipped'})
    g.skip_logs = True

    region = request.args.get('region')
    template_id = request.args.get('id')

    connection = db.get_db_connection(env)
    if not connection:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    cursor = connection.cursor()

    try:
        query = """
        SELECT TEMPLATE_ID, REGION, FORMAT_TYPE, USERNAME, USEREMAIL,
               FILTERS, TEMPLATENAME, WORKORDERID, SHARED,
               SHAREDUSERNAME, COLUMNS
        FROM TEMPLATE_DATA
        """
        filters = []
        bind_params = {}

        if region:
            filters.append("REGION = :region")
            bind_params["region"] = region
        if template_id:
            filters.append("TEMPLATE_ID = :template_id")
            bind_params["template_id"] = template_id

        if filters:
            query += " WHERE " + " AND ".join(filters)

        cursor.execute(query, bind_params)
        rows = cursor.fetchall()

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        connection.close()

    result = []

    for row in rows:
        # Print debug (optional)
        print("Raw filters from DB:", row[5])
        print("Raw columns from DB:", row[10])

        # Parse safely
        try:
            filters_json = json.loads(row[5]) if row[5] else {}
        except Exception as e:
            print("Error parsing filters:", e)
            filters_json = {}

        try:
            columns_json = json.loads(row[10]) if row[10] else []
        except Exception as e:
            print("Error parsing columns:", e)
            columns_json = []

        result.append({
            "template_id": row[0],
            "region": row[1],
            "format_type": row[2],
            "userName": row[3],
            "userEmail": row[4],
            "filters": filters_json,
            "templatename": row[6],
            "workorderid": row[7],
            "shared": bool(row[8]),
            "sharedUserName": row[9],
            "columns": columns_json
        })

    return jsonify(result)
