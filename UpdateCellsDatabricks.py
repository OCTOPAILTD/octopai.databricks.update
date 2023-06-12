import base64
import json

import requests


with open('Config.json', 'r') as f:
    config = json.load(f)

def add_cells_to_notebook(notebook_path,language):
    lineagePythonForPy="HeadersForSpline/lineagePythonForPy.txt"
    lineagePythonForSQL = "HeadersForSpline/lineagePythonForSQL.txt"
    ScalaHeaderForPy = "HeadersForSpline/ScalaHeaderForPy.txt"
    ScalaHeaderForSQL = "HeadersForSpline/ScalaHeaderForSQL.txt"
    try:
        with open(lineagePythonForPy, 'r') as file:
            lineagePythonForPyCnt = file.read()
    except Exception as e:
        print(e)

    try:

        with open(lineagePythonForSQL, 'r') as file:
                lineagePythonForSQLCnt = file.read()
    except Exception as e:
        print(e)

    try:

        with open(ScalaHeaderForPy, 'r') as file:
            ScalaHeaderForPyCnt = file.read()
    except Exception as e:
        print(e)

    try:

        with open(ScalaHeaderForSQL, 'r') as file:
            ScalaHeaderForSQLCnt = file.read()
    except Exception as e:
        print(e)



    # Get notebook content
    url = f'{config["DatabricksWorkspaceURL"]}/api/2.0/workspace/export?path={notebook_path}'
    headers = {
        'Content-Type': 'application/ecmascript',
        'Authorization': f'Bearer {config["Token"]}'
    }

    response = requests.get(url, headers=headers)
    notebook_content = response.text
    notebook_content=json.loads(notebook_content)
    new_text=""
    try:
        content_decoded = base64.b64decode(notebook_content["content"]).decode('utf-8')
        lines = content_decoded.split('\n')

        # Remove the first line
        if lines:
            lines = lines[1:]

        # Join the lines back into a single text string
        new_text = '\n'.join(lines)
        print(new_text)
    except Exception as e:
        print(e)



    # Add cells to the notebook
    updated_content=""
    if language=="PYTHON":
        updated_content=ScalaHeaderForPyCnt +"\n"+lineagePythonForPyCnt+"\n"+new_text
    elif language=="SQL":
        updated_content = ScalaHeaderForSQLCnt  + "\n" + lineagePythonForSQLCnt + "\n" + new_text
    else:
        updated_content = ScalaHeaderForSQLCnt   + "\n" + lineagePythonForSQLCnt + "\n" + new_text
    updated_content_decoded=""
    try:
        updated_content_decoded = base64.b64encode(updated_content.encode('utf-8'))
    except Exception as e:
        print(e)
    # Update the notebook
    url = f"https://adb-7614304971745696.16.azuredatabricks.net/api/2.0/workspace/import?path={notebook_path}"

    try:
        data = {
            "path": notebook_path,
            "language": language,
            "content": updated_content_decoded.decode('utf-8'),
            "overwrite": True  # Set to True if you want to overwrite existing code
        }
    except Exception as e:
        print(e)

    try:

        payload=json.dumps(data)
        isTagged= checkIfAlreadyTagged(new_text,'sc._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking')
        if(isTagged==False):
            response = requests.post(url, headers=headers, data=payload)

            if response.status_code == 200:
                print(f"Cells added to {notebook_path}")
            else:
                print(f"Failed to add cells to {notebook_path}")
    except Exception as e:
        print(e)


def checkIfAlreadyTagged(content,header):
    if content.find(header) != -1:
        return True
    else:
        return False



def print_notebooks(self,path):
    # List contents in the given path
    url = f"https://adb-7614304971745696.16.azuredatabricks.net/api/2.0/workspace/list?path={path}"
    headers = {
        'Content-Type': 'application/ecmascript',
        'Authorization': 'Bearer dapi6025b844f74356b6be5446869ef8c8f5-2'
    }

    response = requests.get(url, headers=headers)
    contents = response.json()

    for item in contents["objects"]:
        item_type = item["object_type"]
        item_path = item["path"]

        if item_type == "DIRECTORY":
            # Recursively process subdirectory
            process_workspace(item_path)
        elif item_type == "NOTEBOOK":
            # Add cells to the notebook
            language=item['language']
            add_cells_to_notebook(item_path,language)


# Start processing the workspace from the root path
process_workspace("/")
