import common_funcs as cf
import json
import os
import sys
from tabulate import tabulate
import re

class XEFRJson(object):

    def __init__(self,conn,download_dir) -> None:
        self.download_dir = download_dir
        self.schemas_json = os.path.join(self.download_dir,"schemas.json")
        self.portals_json = os.path.join(self.download_dir,"portals.json")
        self.config = conn

    def apply_permissions(self,permissions_list,schema_list):
        """
        Apply permissions to a list of schemas
        This must be idem potent, i.e. can be run multiple times without error
        """
        pass

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
    
    def reconcile_schema(self):
        """
        Report on all relevant schema definitions
        """

        results = []
        if 'structure_recon' in self.config:

            if self.config['structure_recon'] == 'all':
                raw_schemas = self.report_items('schemas',False)
                schema_dictlist = [x.values() for x in raw_schemas]
                schemas = list(schema_dictlist[0])
            else:
                schemas = self.config['structure_recon'].split(',')
            
            cf.colour_text(f"Reconciling {len(schemas)} schemas","GREEN")

            for schema in schemas:
                results.append(self.schema_def(schema.strip()))

        for result in results:
            print(result)

        reconcile_file = os.path.join(self.download_dir,f"{self.config['instance']}_recon_schema_defs.txt")

        with open(reconcile_file, 'w') as output_file:
            output_file.write('\n'.join(results))

        cf.colour_text(f"Reconciliation report written to {reconcile_file}","GREEN")

    def schema_def(self,schema_name):
        """
        Text only report, without the attribute ids, used for reconciliation
        Returns a string summary of the schema
        """
        item_type = 'schemas'

        with open(self.schemas_json, 'r') as file:
            data = json.load(file)
            report = ""
            for item in data:

                if item.get("name") == schema_name:

                    report =(f'\nSchema Name: {schema_name}\n')
                    report = report + (f'Description: {item.get("description")}\n')

                    raw_attributes = item.get("attributes")
                    table_data = [[col["name"], col["type"]] for col in raw_attributes]
                    report = report + (tabulate(table_data, headers=["Name", "Type"], tablefmt="plain"))
                    return report
                
    def schema_report(self,schema_name):
        """
        Outputs a formatted report displaying the key information for a schema
        colour formatted including the attribute ids
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
                    cacheType = item.get("cacheType", "n/a")
                    Interval = item.get("cacheRemoteDataInMongoSeconds","n/a")
                    indexes = self.get_index_details(item.get("indexes", "n/a"))
                
                if item_type == 'portals':
                    title = item.get("title")
                    type_class = "n/a"
                    cacheType = "n/a"
                    Interval = "n/a"
                    indexes = "n/a"

                results[id] = [title,type_class,cacheType,Interval,indexes]

        sorted_results = sorted(results.items(), key=lambda x: x[1])
        if display:
            print(f"\nAll {item_type} items\n")
            print("{:<5} {:<45} {:<20} {:<36} {:<8} {:<4} {:<20}".format("#","Name", "Type", "UUID","Cache","Int","Indexes"))
            for i, details in enumerate(sorted_results,1):

                id = details[0]
                name, type_class,cacheType,cacheRemoteDataInMongoSeconds,indexes = details[1]
                print("{:<5} {:<45} {:<20} {:<20} {:<8} {:<4} {:<20}".format(i,name, type_class, id,cacheType,cacheRemoteDataInMongoSeconds,indexes))
        else:
            item_name={}
            item_id={}
            for i, details in enumerate(sorted_results,1):
                item_name[f"s{str(i)}"]   = details[1][0] # name
                item_id[f"s{str(i)}"]   = details[0] # id

            return item_name, item_id

    def get_index_details(self,index_text):
        """Extract key index information"""

        result =[]

        if index_text == "n/a":
            return "n/a"
        
        for index in index_text:
            nos_attributes = len(index["attributeIds"])
            if nos_attributes == 1:
                result.append(f'Single on {index["attributeIds"][0]}')
            else:
                result.append(f':{index["concatenatedFieldName"]}: on {len(index["attributeIds"])} columns')

        return ', '.join(result)


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

    def portal_access(self):
        """
        Returns a list of portals and which roles have access to them
        """

        source_file = self.portals_json

        with open(source_file, 'r') as file:
            data = json.load(file)

            for item in data:

                title = item.get("title")
                roles = item.get("roles")
                cf.colour_text(f"{title} \t {roles}","BLUE")