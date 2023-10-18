import os
import sys
import signal
import json_tools as jt
import common_funcs as cf
import xefr_endpoints

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'xerini_utils'))
sys.path.append(utils_path)

#TODO endpoints only required to download data neatly from XEFR for a given schema

import utilities
import mongo_connector


logging = utilities.MyLogger()
logging.reset_log()
logger = logging.getLogger()
logger.info(f"XEFR CLI: started")

database = 'xefr-signify-dev'
instance = "LOCAL"

mongo = mongo_connector.Mongo(instance,database,logger)
xefr = xefr_endpoints.EndPoints(instance,database,mongo,utilities,logger)

active_mongo_view = "/Users/jasonbraid/dev/xerini/signify_utilities/data/mongoDB views/UK Forecast View.json"

available_commands = {
    "0: Shutdown (hard)": (sys.exit,[]),
    "1: List schemas": (jt.report_items,["schemas"]),
    "2: List portals": (jt.report_items,["portals"]),
    "3: Extract specific schema ['schema name']": (jt.extract_json,["item_list=?","schemas"]),
    "4: Extract specific portal ['portal name']": (jt.extract_json,["item_list=?","portals"]),
    "5: Duplicate schema ['source_name', 'new_name', 'new_name_id']": (jt.copy_schema,["source_name=?","new_name=?","new_name_id=?"]),
    "6: Download schema data ['schema name']": (xefr.download_schemas_data,["schema_list=?"]),
    "7: Download all schema data": (xefr.download_all_schemas_data,[]),
    "8: Confirm active mongoDB view": (print,[active_mongo_view]),
    "9: Display schema data ['schema name']": (xefr.display_schema,["schema_name=?"])
}

script_version = "1.3-OCT23"

# ANSI escape code to clear the terminal screen
CLEAR_SCREEN = "\033c"

def clean_shutdown():
    """
    Handles the clean shutdown of the program
    """
    print("\nExiting the program.")
    mongo.disconnect()
    sys.exit()

# Define a function to handle Ctrl+C (SIGINT)
def signal_handler(sig, frame):
    clean_shutdown()

# Register the signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)

def run_selected_command(desc,func, func_args):
    """
    Handles the derivation of the arguments required to
    run the user selected command.
    Any arugment that has a '?' in the value will be user prompted
    N.B.Automatically downloads the latest schemas / portals.json
    """

    mongo.get_xefr_json("schemas")
    mongo.get_xefr_json("portals")

    contains_question_mark = any('?' in item for item in func_args)

    if contains_question_mark:
        cf.colour_text(f"Function {desc.strip()} requires parameters","GREEN")
        for arg in func_args:
            if '?' in arg:
                cf.colour_text(f"Enter value(s) for argument: {arg}","GREEN")
                user_input = input()
                func_args[func_args.index(arg)] = user_input
            
        _ = func(*func_args)
    else:
        _ = func(*func_args)

def check_command(user_input):
    """
    Maps the user input to the appropriate function
    N.B. User just types the number of the command
    """

    raw_keys = list(available_commands.keys())
    cmd_ids = [int(x.split(':')[0]) for x in raw_keys]
    cmd_labels = [x.split(':')[1] for x in raw_keys]

    if int(user_input) not in cmd_ids:
        display_intro()
        print(f"You typed {user_input} which is not of the command numbers\n{raw_keys}\n")
    else:
        selected_func, arg = available_commands[raw_keys[int(user_input)]]
        run_selected_command(cmd_labels[int(user_input)],selected_func, arg)
        return
    
def display_intro():
    """
    Display the intro text for the CLI
    """
    os.system("clear")
    box_len = 47
    cf.colour_text('_' * box_len,"GREEN")
    cf.colour_text(f"Welcome to the XEFR CLI! [version {script_version}]","GREEN")
    print("Type 'exit', or control c to quit the program.")
    print("Type '? to see a list of commands.")
    cf.colour_text("Select command by the number only.","RED")
    cf.colour_text('_' * box_len,"GREEN")

def command_line_input():

    display_intro()

    while True:
        user_input = input("\nEnter a command or type 'exit' to quit: ")
        if user_input == 'exit':
           clean_shutdown()
        else:
            if user_input =="?":
                print("Available commands:")
                for command in available_commands:
                    print(command)
            else:
                check_command(user_input)

if __name__ == '__main__':
    command_line_input()