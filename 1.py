final_output = your_dict
if "logs" in final_output:
    del final_output["logs"]
return jsonify(final_output)
