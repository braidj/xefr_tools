"""
Collection of functions used by xefre_tools scripts
"""
from datetime import datetime
import os

permitted_types = ['schemas','portals']

text_colours= {
    "RESET": "\033[0m",
    "BOLD": "\033[1m",
    "RED" :"\033[91m",
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "BLUE" :"\033[94m",
    "MAGENTA": "\033[95m",
    "CYAN" :"\033[96m"
}

sort_orders = {
    'Metrics UK NFI Adjustments':'Candidate,Placement,InvoiceDate',
    'TSP UK Invoice Tracker':'Candidate,Placement,InvoiceDate',
    'Metrics GBP Forex Daily':'Year,Month,Day,Symbol',
    'Bullhorn Candidates':'Consultant',
    'Bullhorn Consultant Details':'Consultant'
}

def add_ts_prefix(full_file_path):
    """
    Add a timestamp prefix to file name component of a file path
    """

    now = datetime.now()
    timestamp = now.strftime("%Y_%m_%d_%H%M%S")
    directory, filename = os.path.split(full_file_path)
    new_filename = f"{timestamp}_{filename}"
    updated_file_path = os.path.join(directory, new_filename)
    return updated_file_path

def colour_text(text, colour):
    """
    Colour the text using ANSI escape codes
    """
    print (f"{text_colours[colour]}{text}{text_colours['RESET']}")

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

