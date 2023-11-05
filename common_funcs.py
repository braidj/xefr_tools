"""
Collection of functions used by xefre_tools scripts
"""
from datetime import datetime
import os
import sys
import subprocess
import psutil
import signal

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

# Hide these columns when displaying schema data
hide_columns = {
    'View UK Forecast': (
        'PlacementCandidateLookup,Multiplier,CompanyName,'
        'Placement Start,Placement End,Year,Quarter,Day,Source,'
        'Currency,Country,Month,Forecast Cut Off,ExchangeRate,ChargeCode,LinePrice'
    ),
    'View UK NFI': (
        'Type,PlacementCandidateLookup,ChargeCode,CompanyName,'
        'Quarter,Day,Consultant'
    ),
    'View US NFI': (
        'Type,PlacementCandidateLookup,ChargeCode,Multiplier,CompanyName,'
        'Placement Start,Placement End,Quarter,Day,WorkingDaysCK,Source,'
        'Currency,Country,Consultant'
    ),
    'View UK NFI Forecast': (
        'Type,PlacementCandidateLookup,ChargeCode,Multiplier,CompanyName,'
        'Placement Start,Placement End,Source,'
        'Currency,Country,Consultant'
    )
}

def kill_all_previous_instances(script_name):
    """
    Kill all but the most recent instance of the supplied script name
    """
    pid_history,last_pid = get_running_processes(script_name)
    for pid in pid_history:
        if pid != last_pid:
            print(f"Killing {pid}")
            kill_process(pid)

def kill_process(pid):
    """
    Kill the process with the supplied pid
    """
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass    

def get_pid_create_time(pid):
    """
    Get the create time for the supplied pid
    """
    try:
        process = psutil.Process(pid)
        create_time = process.create_time()
        return create_time
    except psutil.NoSuchProcess:
        return None

def get_running_processes(script_name,display=False):
    """
    Returns an array of the running pids for the supplied script name
    """
    pid_history={}
    pids = []
    try:
        command = f"ps aux | grep '{script_name}' | grep -v grep"
        result = subprocess.check_output(command, shell=True, text=True)
        lines = result.strip().split('\n') 
        pids.append(int(line.split()[1]))  # Extract PIDs
    except subprocess.CalledProcessError:
        pass

    for pid in pids:
        pid_history[pid] = get_pid_create_time(pid)

    sorted_dict = dict(sorted(pid_history.items(), key=lambda item: item[1]))
    last_pid = list(sorted_dict.keys())[-1]
    return sorted_dict,last_pid
    
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

if __name__ == "__main__":
    print("This module is not intended to be run stand-alone")
    print("Only use to test new fuctions")
