import os
import sys
import signal
import json_tools as jt
import common_funcs as cf
import xefr_endpoints
import types

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
command_history = {"last_command": "not run yet"} # re-run the last command easily
full_script_path = os.path.abspath(__file__)

available_commands = {
    "0: List schemas": (jt.report_items,("schemas")),
    "1: Schema Report ['schema name']": (jt.schema_report,("schema_name=?")),
    "2: Display schema data ['schema name']": (xefr.display_schema,("schema_name=?")),
    "3: Download schema data ['schema name']": (xefr.download_schemas_data,("schema_name=?")),
    "4: Extract specific schema ['schema name']": (jt.extract_json,("object_name=?","schemas")),
    "5: Duplicate schema ['source_name', 'new_name', 'new_name_id']": (jt.copy_schema,("source_name=?","new_name=?","new_name_id=?")),
    "6: Download all schema data": (xefr.download_all_schemas_data,()),
    "7: Display Pipeline columns ['schema name']": (jt.get_pipeline_columns,("schema_name=?")),
    "8: List portals": (jt.report_items,("portals")),
    "9: Extract specific portal ['portal name']": (jt.extract_json,("object_name=?","portals"))
}

command_raw_keys = list(available_commands.keys())
command_attributes = list(available_commands.values())
command_ids = [(x.split(':')[0]) for x in command_raw_keys]
permitted_str_commands = ['x','?','r']
command_descriptions= [x.split(':')[1] for x in command_raw_keys]
all_permitted_command_ids = command_ids + permitted_str_commands

script_version = "1.4-OCT23"

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

def run_selected_command(cmd_id):
    """
    Handles the derivation of the arguments required to
    run the user selected command.
    Any argument that has a '?' in the value will be user prompted
    N.B.Automatically downloads the latest schemas / portals.json
    """

    selected_func,template_args = command_attributes[cmd_id]

    if type(template_args) == tuple:
        template_args_list = list(template_args)
    if type(template_args) == str:
        template_args_list = template_args.split(',')
    if type(template_args) == dict:
        template_args_list = [template_args]

    func_desc = command_descriptions[cmd_id]

    args_list=[] # used to store the arguments to pass to the function

    mongo.get_xefr_json("schemas")
    mongo.get_xefr_json("portals")

    for arg in template_args_list:

        if '?' in arg:
            cf.colour_text(f"{func_desc}: Enter value(s) for: {arg}","GREEN")
            user_input = input()

            if len(template_args_list) == 1: # only one arg, so postional style not required
                arg = user_input
            else:
                arg = arg.replace('?',user_input)

        args_list.append(arg)

    command_history['last_command'] = [selected_func,args_list]

    _ = selected_func(*args_list)

def rerun_last_command():
    """
    RE-runs the last command
    """
    cf.colour_text("Re-running the last command","BLUE")
    selected_func,args_list = command_history['last_command']

    _ = selected_func(*args_list)

def check_command(user_input):
    """
    Maps the user input to the appropriate function
    N.B. User just types the number of the command
    To pass it to commands library needs to be a int
    """

    selected_cmd_id = user_input.strip()

    if selected_cmd_id not in all_permitted_command_ids:
        print(f"You typed {user_input} which is not of the permitted  commands\n{all_permitted_command_ids}\n")
        display_intro()
        return

    if selected_cmd_id in permitted_str_commands:
        if user_input == '?':
            display_commands()
            return
        elif user_input == 'x':
            clean_shutdown()
            return
        elif user_input == 'r':
            rerun_last_command()
            return

    run_selected_command(int(selected_cmd_id))
    return

def display_commands():
    """
    Display the available commands
    """
    print("Available commands:")

    for command in available_commands:
        print(command)

def display_intro():
    """
    Display the intro text for the CLI
    """
    os.system("clear")
    box_len = 47
    cf.colour_text('_' * box_len,"GREEN")
    cf.colour_text(f"Welcome to the XEFR CLI! [version {script_version}]","GREEN")
    print("Type 'x', or control c to quit the program.")
    print("Type '? to see a list of commands.")
    cf.colour_text("Select command by the number only.","RED")
    cf.colour_text('_' * box_len,"GREEN")

def command_line_input():

    display_intro()

    while True:

        user_input = input("\nEnter a command or type 'x' to quit: ")
        check_command(user_input)

if __name__ == '__main__':
    command_line_input()