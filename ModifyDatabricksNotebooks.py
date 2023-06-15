import json
from getpass import getpass

import requests

from Logger import Logger
from ModifyCellsDatabricks import NotebookManager


class Notebook:
    def __init__(self, name, workspace_url, add_cells):
        self.name = name
        self.workspace_url = workspace_url
        self.add_cells = add_cells



class TokenValidator:
    def is_valid_token(self, token,workspaceURL):

            path="/"
            url = f"{workspaceURL}/api/2.0/workspace/list?path={path}"
            headers = {
                'Authorization': 'Bearer ' + token,
                'Content-Type': 'application/ecmascript'


            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                return True
            else:
                log.error('Failed to log in with token.')
                return False


def write_to_file(filename, content):
    try:
        with open(filename, 'w') as file:
            file.write(content)
        log.info(f"Successfully wrote content to {filename}.")
    except Exception as e:
        log.error(f"Failed to write to file: {e}")

def read_from_file(filename):
    try:
        with open(filename, 'r') as file:
            return file.read()
        log.info(f"Successfully wrote content to {filename}.")
    except Exception as e:
        log.error(f"Failed to write to file: {e}")
def main():
    token_validator = TokenValidator()

    workspaceUrl=input("Enter yout workspaceurl:")
    token = getpass("Enter your token: ")


    if token_validator.is_valid_token(token,workspaceUrl):
        # Create an instance of the NotebookManager class
        notebook_manager = NotebookManager(token,workspaceUrl,log)

        while True:
            choice = input("Choose an option:\n1. Create list of notebooks\n2. Add text to notebooks in a JSON file\n3.Exit\n")
            if choice == '1':
                   listOfDict=[]
                   notebook_manager.print_workspace("/",listOfDict)
                   txt=json.dumps(listOfDict)
                   fileName="notebooks.json"
                   write_to_file(fileName,txt)
                   log.info(f"json file was created:{fileName}")
                   log.info(f"number of notebooks:{len(listOfDict)}")
            elif choice == '2':
                json_file = input("Enter the path to the JSON file to run the update: ")
                txt=read_from_file(json_file)
                dictx=json.loads(txt)
                listOfDictsUpdated=[]
                notebook_manager.modify_workspace("/",dictx,listOfDictsUpdated)
                log.info(f"number of updated notebooks:{len(listOfDictsUpdated)}")
            elif choice == '3':
                print("Exit")
                break


            else:
                log.error("Invalid choice.")
    else:
        log.error("Invalid token.")

log=Logger('ModifyDatabricks')
main()