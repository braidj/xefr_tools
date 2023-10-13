"""
Collection of functions used by xefre_tools scripts
"""

import os

permitted_types = ['schemas','portals']

def get_download_directory():
    """
    Get the user's home directory
    """
    home_directory = os.path.expanduser("~")

    download_directory = os.path.join(home_directory, "Downloads")

    return download_directory

def get_xefr_directory():
    """
    Get the xefr download directory
    """

    xefr_directory = os.path.join(get_download_directory(), "xefr")

    return xefr_directory

def get_output_json(item_type, item_name):
    """
    Get the output json file for the item type and name
    """
    if item_type not in permitted_types:
        raise Exception(f"Type {item_type} not permitted, only {permitted_types}")
    
    output_file = f"{get_xefr_directory()}/{item_type} {item_name}.json"

    return output_file

def get_source_json(item_type):
    """
    Get the source json file for the item type
    """
    if item_type not in permitted_types:
        raise Exception(f"Type {item_type} not permitted, only {permitted_types}")
    
    source_file = f"{get_xefr_directory()}/{item_type}.json"

    return source_file

