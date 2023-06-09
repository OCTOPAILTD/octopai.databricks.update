import base64
import json
import requests

class NotebookManager:
    def __init__(self,token,workspaceURL,logger):

        self.token=token
        self.workspaceURL=workspaceURL
        self.logger=logger

    def add_cells_to_notebook(self, notebook_path, language):

        lineagePythonForPy = "HeadersForSpline/lineagePythonForPy.txt"
        lineagePythonForSQL = "HeadersForSpline/lineagePythonForSQL.txt"
        ScalaHeaderForPy = "HeadersForSpline/ScalaHeaderForPy.txt"
        ScalaHeaderForSQL = "HeadersForSpline/ScalaHeaderForSQL.txt"
        try:
            with open(lineagePythonForPy, 'r') as file:
                lineagePythonForPyCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        try:
            with open(lineagePythonForSQL, 'r') as file:
                lineagePythonForSQLCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        try:
            with open(ScalaHeaderForPy, 'r') as file:
                ScalaHeaderForPyCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        try:
            with open(ScalaHeaderForSQL, 'r') as file:
                ScalaHeaderForSQLCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        # Get notebook content
        url = f'{self.workspaceURL}/api/2.0/workspace/export?path={notebook_path}'
        headers = {
            'Content-Type': 'application/ecmascript',
            'Authorization': f'Bearer {self.token}'
        }

        response = requests.get(url, headers=headers)
        notebook_content = response.text
        notebook_content = json.loads(notebook_content)
        new_text = ""
        try:
            content_decoded = base64.b64decode(notebook_content["content"]).decode('utf-8')
            lines = content_decoded.split('\n')

            # Remove the first line
            if lines:
                lines = lines[1:]

            # Join the lines back into a single text string
            new_text = '\n'.join(lines)
            self.logger.info(new_text)
        except Exception as e:
            self.logger.info(e)

        # Add cells to the notebook
        updated_content = ""
        if language == "PYTHON":
            updated_content = ScalaHeaderForPyCnt + "\n" + lineagePythonForPyCnt + "\n" + new_text
        elif language == "SQL":
            updated_content = ScalaHeaderForSQLCnt + "\n" + lineagePythonForSQLCnt + "\n" + new_text
        else:
            updated_content = ScalaHeaderForSQLCnt + "\n" + lineagePythonForSQLCnt + "\n" + new_text
        updated_content_decoded = ""
        try:
            updated_content_decoded = base64.b64encode(updated_content.encode('utf-8'))
        except Exception as e:
            self.logger.error(e)

        # Update the notebook
        url = f"{self.workspaceURL}/api/2.0/workspace/import?path={notebook_path}"

        try:
            data = {
                "path": notebook_path,
                "language": language,
                "content": updated_content_decoded.decode('utf-8'),
                "overwrite": True  # Set to True if you want to overwrite existing code
            }
        except Exception as e:
            self.logger.error(e)

        try:
            payload = json.dumps(data)
            is_tagged = self.check_if_already_tagged(new_text,'sc._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking')
            if not is_tagged:
                response = requests.post(url, headers=headers, data=payload)

                if response.status_code == 200:
                    self.logger.info(f"Cells added to {notebook_path}")
                else:
                    self.logger.info(f"Failed to add cells to {notebook_path}")
        except Exception as e:
            self.logger.error(e)

    def check_if_already_tagged(self, content, header):
        if content.find(header) != -1:
            return True
        else:
            return False

    def process_workspace(self, path):
        # List contents in the given path
        url = f"{self.workspaceURL}/api/2.0/workspace/list?path={path}"
        headers = {
            'Content-Type': 'application/ecmascript',
            'Authorization': f'Bearer {self.token}'
        }

        response = requests.get(url, headers=headers)
        contents = response.json()

        for item in contents["objects"]:
            item_type = item["object_type"]
            item_path = item["path"]

            if item_type == "DIRECTORY":
                # Recursively process subdirectory
                self.process_workspace(item_path)
            elif item_type == "NOTEBOOK":
                # Add cells to the notebook
                language = item['language']
                self.add_cells_to_notebook(item_path, language)

    def print_workspace(self, path, dict_of_notebooks):
        try:
            self._print_workspace_recursive(path, dict_of_notebooks, offset=0)
        except Exception as e:
            self.logger.error(e)

    def _print_workspace_recursive(self, path, dict_of_notebooks, offset=0, limit=1000):
        try:
            url = f"{self.workspaceURL}/api/2.0/workspace/list?path={path}"
            headers = {
                'Content-Type': 'application/ecmascript',
                'Authorization': f'Bearer {self.token}'
            }

            response = requests.get(url, headers=headers)
            contents = response.json()

            if  len(contents) > 0:
                for item in contents["objects"]:
                    item_type = item["object_type"]
                    item_path = item["path"]

                    if item_type == "DIRECTORY":
                        # Recursively process subdirectory
                        self._print_workspace_recursive(item_path, dict_of_notebooks)
                    elif item_type == "NOTEBOOK":
                        # Add cells to the notebook
                        language = item['language']
                        dict_of_notebooks.append({
                            "workspaceURL": self.workspaceURL,
                            "notebookPath": item_path,
                            "language": language,
                            "isAutomate":"True"
                        })
                        self.logger.info(item_path)

            # Check if there are more items to fetch
            # if "has_more" in contents and contents["has_more"]:
            #     next_offset = offset + limit
            #     self._print_workspace_recursive(path, dict_of_notebooks, offset=next_offset)
        except Exception as e:
            self.logger.error(e)

    def modify_workspace(self, listOfDicts,listOfDictsUpdated):
        try:
            listOfPaths= [  {"workspaceURL":x["workspaceURL"],
                            "notebookPath": x["notebookPath"],
                            "language":x["language"],
                            "isAutomate": x["isAutomate"]} for x in listOfDicts if eval(x["isAutomate"]) == True]

            for notebook in listOfPaths:
                listOfDictsUpdated.append({"notebookPath": notebook["notebookPath"], "workspacseUrl": notebook["workspaceURL"]})
                self.add_cells_to_notebook(notebook["notebookPath"], notebook["language"])
                self.logger.info(notebook["notebookPath"])

            self.logger.info(f"Number of notebooks updated:{len(listOfDictsUpdated)}")
        except Exception as e:
            self.logger.error(e)

    def add_cells_to_notebook(self,notebook_path, language):
        lineagePythonForPy = "HeadersForSpline/lineagePythonForPy.txt"
        lineagePythonForSQL = "HeadersForSpline/lineagePythonForSQL.txt"
        ScalaHeaderForPy = "HeadersForSpline/ScalaHeaderForPy.txt"
        ScalaHeaderForSQL = "HeadersForSpline/ScalaHeaderForSQL.txt"
        try:
            with open(lineagePythonForPy, 'r') as file:
                lineagePythonForPyCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        try:

            with open(lineagePythonForSQL, 'r') as file:
                lineagePythonForSQLCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        try:

            with open(ScalaHeaderForPy, 'r') as file:
                ScalaHeaderForPyCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        try:

            with open(ScalaHeaderForSQL, 'r') as file:
                ScalaHeaderForSQLCnt = file.read()
        except Exception as e:
            self.logger.error(e)

        # Get notebook content
        url = f'{self.workspaceURL}/api/2.0/workspace/export?path={notebook_path}'
        headers = {
            'Content-Type': 'application/ecmascript',
            'Authorization': f'Bearer {self.token}'
        }

        response = requests.get(url, headers=headers)
        notebook_content = response.text
        notebook_content = json.loads(notebook_content)
        new_text = ""
        try:
            if not "error_code" in notebook_content:
                content_decoded = base64.b64decode(notebook_content["content"]).decode('utf-8')
                lines = content_decoded.split('\n')

                # Remove the first line
                if lines:
                    lines = lines[1:]

                # Join the lines back into a single text string
                new_text = '\n'.join(lines)
                self.logger.error(new_text)
            else :
                if not "error_code" in notebook_content:
                    self.logger.error(["error_code"])

        except Exception as e:
            self.logger.error(e)

        # Add cells to the notebook
        updated_content = ""
        if language == "PYTHON":
            updated_content = ScalaHeaderForPyCnt + "\n" + lineagePythonForPyCnt + "\n" + new_text
        elif language == "SQL":
            updated_content = ScalaHeaderForSQLCnt + "\n" + lineagePythonForSQLCnt + "\n" + new_text
        else:
            updated_content = ScalaHeaderForSQLCnt + "\n" + lineagePythonForSQLCnt + "\n" + new_text
        updated_content_decoded = ""
        try:
            updated_content_decoded = base64.b64encode(updated_content.encode('utf-8'))
        except Exception as e:
            self.logger.error(e)
        # Update the notebook
        url = f"{self.workspaceURL}/api/2.0/workspace/import?path={notebook_path}"

        try:
            data = {
                "path": notebook_path,
                "language": language,
                "content": updated_content_decoded.decode('utf-8'),
                "overwrite": True  # Set to True if you want to overwrite existing code
            }
        except Exception as e:
            self.logger.error(e)

        try:

            payload = json.dumps(data)
            isTagged = self.checkIfAlreadyTagged(new_text,
                                            'sc._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking')
            if (isTagged == False):
                response = requests.post(url, headers=headers, data=payload)

                if response.status_code == 200:
                    print(f"Cells added to {notebook_path}")
                else:
                    print(f"Failed to add cells to {notebook_path}")
        except Exception as e:
            self.logger.error(e)

    def checkIfAlreadyTagged(self,content, header):
        if content.find(header) != -1:
            return True
        else:
            return False



