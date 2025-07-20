@app.route('/GetFilterColumns', methods=['GET'])
def get_filter_columns_by_region():
    if getattr(g, 'skip_logs', False):
            return response
    g.skip_logs = True
    connection = db.get_db_connection(env)
    if not connection:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    cursor = connection.cursor()

    try:
        query = """
            SELECT ID, "value", "label", "filter_type", "is_active", "default", "REGION"
            FROM REGION_DATA_FILTERS
        """
        cursor.execute(query)
        rows = cursor.fetchall()
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        connection.close()

    def convert_to_bool(value):
        return str(value).strip() == '1'

    result = {}

    for row in rows:
        region = row[6].strip() if row[6] else "UNKNOWN"
        filter_item = {
            "id": row[0],
            "value": row[1],
            "label": row[2],
            "type": row[3],
            "default": convert_to_bool(row[5]),
            "is_active": convert_to_bool(row[4])
        }
        if region not in result:
            result[region] = []
        result[region].append(filter_item)

    return jsonify(result) 
