import os
import sys
import signal


# ANSI escape code to clear the terminal screen
CLEAR_SCREEN = "\033c"

# Define a function to handle Ctrl+C (SIGINT)
def signal_handler(sig, frame):
    print("\nExiting the program.")
    sys.exit()

# Register the signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)

def execute_command(cmd_id):

    commands = cli_commands.available_commands
    
    if cmd_id == 'help':
        for i, cmd in enumerate(commands):

            if type(cmd) == str:
                label = cmd
            else:
                label = cmd.__name__

            print(f"{i}: {label}")
    else:
        print(f"You entered: {cmd_id}")

def command_line_input():

    # Clear the terminal screen
    os.system("clear")

    print("Welcome to the XEFR CLI!")
    print("Type 'exit', or control c to quit the program.")
    print("Type 'help' or ? to see a list of commands.")

    while True:
        user_input = input("Enter a command or type 'exit' to quit: ")
        if user_input == 'exit':
           cli_commands.stop_me()
        else:
            execute_command(user_input)

if __name__ == '__main__':
    command_line_input()