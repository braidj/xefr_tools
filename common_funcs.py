"""
Collection of functions used by xefr_tools scripts
"""
from datetime import datetime
import os
import subprocess
import psutil
import signal
import pandas as pd
import hashlib


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

def generate_md5_checksum(file_path):
    # Creating a hash object
    md5_hash = hashlib.md5()

    # Open the file to read it's bytes
    with open(file_path, "rb") as f:
        # Reading and updating hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    
    # Returning the complete hash
    schema_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"{schema_name} = {md5_hash.hexdigest()}")

def sum_numeric_columns(csv_file):
    """Sum up all numeric columns in the supplied csv file"""

    df = pd.read_csv(csv_file)

    sum_dict = df.select_dtypes(include=['number']).sum()

    for column, total in sum_dict.items():
        formatted_total = "{:,.2f}".format(total)
        print(f"Total for {column}: {formatted_total}")

def colour_text(text, colour):
    """
    Colour the text using ANSI escape codes
    """
    print (f"{text_colours[colour]}{text}{text_colours['RESET']}")

# Used when testing out MongoDB Views
#------------------------------------
sort_orders = {
    'Metrics UK NFI Adjustments':'Candidate,Placement,InvoiceDate',
    'TSP UK Invoice Tracker':'Candidate,Placement,InvoiceDate',
    'Metrics GBP Forex Daily':'Year,Month,Day,Symbol',
    'Bullhorn Candidates':'Consultant',
    'Bullhorn Consultant Details':'Consultant'
}

# If schema in list then hide coluumns method
hide_schemas = [ 'View UK NFI','View US NFI','View UK Forecast']

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

show_columns={
   'View UK NFI Forecast': (
        'Consultant,Country,Team,Team Lead'
    )  
}
#------------------------------------

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
        pids.append(int(lines.split()[1]))  # Extract PIDs
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
    timestamp = now.strftime("%d%b%y_%H%M")
    directory, filename = os.path.split(full_file_path)
    new_filename = f"{timestamp}_{filename}"
    updated_file_path = os.path.join(directory, new_filename)
    return updated_file_path

def setup_local_folder(instance, database):
    """
    Check if the local folder exists for the supplied instance and database
    and create if necessary
    Returns the path to the local folder
    This XEFR folder exists within the downloads folder
    """

    download = get_download_directory()
    source = f"{instance}_{database}"

    local_folder = os.path.join(download, "xefr",source)

    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    return local_folder

def get_download_directory():
    """
    Get the user's home directory
    """
    home_directory = os.path.expanduser("~")
    download_directory = os.path.join(home_directory, "Downloads")
    return download_directory

if __name__ == "__main__":
    print("This module is not intended to be run stand-alone")
    print("Only use to test new fuctions")
