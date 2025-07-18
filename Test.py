def get_config_value(region, key, configPath):
    region = region.upper()
    key = key.upper()

    if key == "FID":
        if region == "DAO":
            return configPath.get('Linkage_DAO')
        elif region == "APJ":
            return configPath.get('Linkage_APJ')
        elif region == "EMEA":
            return configPath.get('Linkage_EMEA')

    elif key == "FOID":
        if region == "DAO":
            return configPath.get('FM_Order_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('FM_Order_EMEA_APJ')

    elif key == "SOPATH":
        if region == "DAO":
            return configPath.get('SO_Header_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('SO_Header_EMEA_APJ')

    elif key == "WOID":
        if region == "DAO":
            return configPath.get('WO_Details_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('WO_Details_EMEA_APJ')

    elif key == "FFBOM":
        if region == "DAO":
            return configPath.get('FM_BOM_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('FM_BOM_EMEA_APJ')

    return None  # If no matching condition is found
