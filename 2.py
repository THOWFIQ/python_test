def get_install_instruction2_id(fulfillment_entry):
    # If the entry is already a list, take the first dict inside it
    if isinstance(fulfillment_entry, list):
        fulfills = []
        for f in fulfillment_entry:
            fulfills.extend(listify(f.get("fulfillments", [])))
    elif isinstance(fulfillment_entry, dict):
        fulfills = listify(fulfillment_entry.get("fulfillments", []))
    else:
        fulfills = []

    for fulfill in fulfills:
        instruction = fulfill.get("installInstruction2Id")
        if instruction:
            return instruction
    return None
