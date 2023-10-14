import os
import sys
import signal
import json_tools as jt
import common_funcs as cf

available_commands = {
    "0: list schemas": (jt.report_items,["schemas"]),
    "1: list portals": (jt.report_items,["portals"]),
    "2: extract schema ['schema name']": (jt.extract_json,["item_list=?","schemas"]),
    "3: extract portal ['portal name']": (jt.extract_json,["item_list=?","portals"])
}

script_version = "1.0-OCT23"

# ANSI escape code to clear the terminal screen
CLEAR_SCREEN = "\033c"

# Define a function to handle Ctrl+C (SIGINT)
def signal_handler(sig, frame):
    print("\nExiting the program.")
    sys.exit()

# Register the signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)

def run_selected_command(desc,func, func_args):
    """
    Handles the derivation of the arguments required to
    run the user selected command.
    Any arugment that has a '?' in the value will be user prompted
    """

    contains_question_mark = any('?' in item for item in func_args)

    if contains_question_mark:

        for arg in func_args:
            if '?' in arg:
                print(f"For {desc.strip()} enter value(s) for argument: {arg}")
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
           sys.exit()
        else:
            if user_input =="?":
                print("Available commands:")
                for command in available_commands:
                    print(command)
            else:
                check_command(user_input)

if __name__ == '__main__':
    command_line_input()