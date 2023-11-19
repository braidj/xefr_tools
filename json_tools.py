import common_funcs as cf
import json
import os
from tabulate import tabulate
import re

class XEFRJson(object):

    def __init__(self,download_dir) -> None:
        self.download_dir = download_dir
        self.schemas_json = os.path.join(self.download_dir,"schemas.json")
        self.portals_json = os.path.join(self.download_dir,"portals.json")

    def __get_source_file(self,item_type):
        """
        Returns the source file for the item type
        """
        if item_type not in cf.permitted_types:
            raise Exception(f"Type {item_type} not permitted, only {cf.permitted_types}")
        
        if item_type == 'schemas':
            source_file = self.schemas_json
        elif item_type == 'portals':
            source_file = self.portals_json

        return source_file

    def schema_report(self,schema_name):
        """
        Outputs a formatted report displaying the key information for a schema
        """
        item_type = 'schemas'
        schema = schema_name.strip()

        with open(self.schemas_json, 'r') as file:
            data = json.load(file)

            for item in data:

                if item.get("name") == schema:

                    cf.colour_text(f'Schema Name: {schema}',"BLUE")
                    cf.colour_text(f'Description: {item.get("description")}',"BLUE")

                    raw_attributes = item.get("attributes")
                    table_data = [[col["name"], col["type"], col["id"]] for col in raw_attributes]
                    print(tabulate(table_data, headers=["Name", "Type", "ID"], tablefmt="grid"))

    def schema_report(self,schema_name):
        """
        Outputs a formatted report displaying the key information for a schema
        """

        item_type = 'schemas'
        schema = schema_name.strip()

        with open(self.schemas_json, 'r') as file:
            data = json.load(file)

            for item in data:

                if item.get("name") == schema:

                    cf.colour_text(f'Schema Name: {schema}',"BLUE")
                    cf.colour_text(f'Description: {item.get("description")}',"BLUE")

                    raw_attributes = item.get("attributes")
                    table_data = [[col["name"], col["type"], col["id"]] for col in raw_attributes]
                    print(tabulate(table_data, headers=["Name", "Type", "ID"], tablefmt="grid"))

    def report_items(self,item_type,display=True):
        """
        Returns alpahbetical list of items of type item_type
        If display is set to True, will print the list
        If display is set to False, will return a dictionary of the items,
        one for name, one for id
        """
        if item_type not in cf.permitted_types:
            raise Exception(f"Type {item_type} not permitted, only {cf.permitted_types}")
        
        source_file = self.__get_source_file(item_type)

        results = {}

        with open(source_file, 'r') as file:
            data = json.load(file)

            for item in data:

                id = item.get("id")

                if item_type == 'schemas':
                    title = item.get("name")
                    type_class = item.get("typeClass")
                    type_class = type_class.replace("com.xerini.xefr.model.","")
                
                if item_type == 'portals':
                    title = item.get("title")
                    type_class = "n/a"

                results[id] = [title,type_class]

        sorted_results = sorted(results.items(), key=lambda x: x[1])
        if display:
            print(f"\nAll {item_type} items\n")
            for i, details in enumerate(sorted_results,1):

                id = details[0]
                name, type_class = details[1]
                print("{:<5} {:<35} {:<20} {:<20}".format(i,name, type_class, id))
        else:
            item_name={}
            item_id={}
            for i, details in enumerate(sorted_results,1):
                item_name[f"s{str(i)}"]   = details[1][0] # name
                item_id[f"s{str(i)}"]   = details[0] # id

            return item_name, item_id

    def extract_json(self,object_name,item_type,backup=True):
        """
        Handle extracting the schema(s) or portal(s) from the 
        source json files and writing them to individual files
        If backup is set as True, will add a timestamp to the name
        """
        if item_type not in cf.permitted_types:
            raise Exception(f"Type {item_type} not permitted, only {cf.permitted_types}")
        
        source_file = self.__get_source_file(item_type)

        with open(source_file, 'r') as file:
            data = json.load(file)

            for item in data:

                if item_type == 'schemas':
                    title = item.get("name")

                if item_type == 'portals':
                    title = item.get("title")

                if title in object_name:
                    

                    # outputfile = cf.get_output_json(item_type, title)
                    outputfile = os.path.join(self.download_dir,f"{item_type} {title}.json")

                    if backup:
                        outputfile = cf.add_ts_prefix(outputfile)

                    with open(outputfile, 'w') as file:
                        try:
                            json.dump([item], file, indent=4)
                            print("{:<4} {:<20} {:<5} {:<20}".format(item_type,title, '---->',outputfile))

                        except Exception as e:
                            print(f"Error writing {title} to {outputfile}")
                            print(e)

    def copy_schema(self, source_name, new_name, new_name_id):
        """
        Copy an existing schema to a new schema, can handle mongo schemas
        N.B. The new schema must already exist.
        Results in new file being created
        """

        source_file = self.schemas_json

        with open(source_file, 'r') as file:
            data = json.load(file)

            for item in data:

                id = item.get("id")
                title = item.get("name")

                if title == source_name:

                    item["id"] = new_name_id
                    item["name"] = new_name

                    if "pipelineText" in item:
                        pipeline_str = item.get("pipelineText")
                        pipeline_str = pipeline_str.replace(source_name, new_name)
                        item["pipelineText"] = pipeline_str

                    outputfile= os.path.join(self.download_dir,new_name+".json")

                    with open(outputfile, 'w') as file:
                        try:
                            json.dump([item], file, indent=4)
                            print("{:<20} {:<20} {:<5} {:<20}".format(new_name,new_name_id, '---->',outputfile))

                        except Exception as e:
                            print(f"Error writing {title} to {outputfile}")
                            print(e)

    def get_pipeline_text(self,schema_name,display=False):
        """
        Returns the pipeline text for a given schema
        Note use of display to print the pipeline text only
        """
        source_file = self.schemas_json

        with open(source_file, 'r') as file:
            data = json.load(file)

            for item in data:

                title = item.get("name")

                if title == schema_name:

                    if "pipelineText" in item:
                        pipeline_str = item.get("pipelineText")

                        if display:
                            cf.colour_text(pipeline_str,"BLUE")
                        else:
                            return pipeline_str

    def get_pipeline_columns(self,schema_name):
        """
        Returns a sorted, distinct list of columns referenced in the pipeline
        """
        schema = schema_name.strip()
        pipeline = self.get_pipeline_text(schema)
        pattern = r'\{([^}]+)\}'

        matches = re.findall(pattern, pipeline)
        distinct_sorted_matches = sorted(set(matches))# Remove duplicates, sort the matches

        column_details = [x for x in distinct_sorted_matches if "$" not in x]
        all_tables = [x.split('.')[0] for x in column_details]
        distinct_tables = sorted(set(all_tables))

        cf.colour_text(f"{schema} pipeline depends on {len(distinct_tables)} tables","BLUE")
        for table_nos, table in enumerate(distinct_tables,1):
            print()
            cf.colour_text(f"{table_nos}: {table}","BLUE")

            for col in column_details:
                if col.startswith(table):
                    print(f"\t\t{col}")
